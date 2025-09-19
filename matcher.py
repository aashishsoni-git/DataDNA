# matcher.py
from rapidfuzz import fuzz
import numpy as np

def safe_float(x):
    try:
        return float(x)
    except:
        return 0.0

def name_similarity(a, b):
    if not a or not b:
        return 0.0
    return fuzz.token_set_ratio(a, b) / 100.0

def jaccard_set(a, b):
    if not a or not b:
        return 0.0
    sa, sb = set([x for x in a if x is not None]), set([x for x in b if x is not None])
    if not sa and not sb:
        return 0.0
    inter = len(sa & sb)
    uni = len(sa | sb)
    return inter / uni if uni > 0 else 0.0

def cosine_sim(e1, e2):
    if e1 is None or e2 is None:
        return 0.0
    v1 = np.array(e1, dtype=float)
    v2 = np.array(e2, dtype=float)
    if v1.size == 0 or v2.size == 0:
        return 0.0
    denom = np.linalg.norm(v1) * np.linalg.norm(v2)
    if denom == 0:
        return 0.0
    return float(np.dot(v1, v2) / denom)

def type_compatible(p1, p2):
    """
    Return (compatible:bool, reason:str)
    Basic rules:
    - If both are explicit patterns (DATE, EMAIL, PHONE, NUMERIC) and not equal -> incompatible
    - NAME vs CATEGORICAL/ALPHANUMERIC ok
    - If one is NUMERIC and other is NAME/EMAIL -> incompatible
    """
    pattern1 = (p1.get("pattern") or "").upper()
    pattern2 = (p2.get("pattern") or "").upper()

    numeric_like = {"NUMERIC","PHONE"}
    date_like = {"DATE_YYYY-MM-DD","DATE_DDMMYYYY"}
    exact_matches = {pattern1 == pattern2}

    # if exact same explicit pattern -> compatible
    if pattern1 == pattern2 and pattern1 != "ALPHANUMERIC" and pattern1 != "UNKNOWN":
        return True, "same_pattern"

    # numeric vs non-numeric -> incompatible
    if (pattern1 in numeric_like and pattern2 not in numeric_like) or (pattern2 in numeric_like and pattern1 not in numeric_like):
        return False, "numeric_mismatch"

    # date vs non-date -> incompatible
    if (pattern1 in date_like and pattern2 not in date_like) or (pattern2 in date_like and pattern1 not in date_like):
        return False, "date_mismatch"

    # email mismatch
    if (pattern1 == "EMAIL" and pattern2 != "EMAIL") or (pattern2 == "EMAIL" and pattern1 != "EMAIL"):
        return False, "email_mismatch"

    return True, "compatible"

def profile_similarity(p1, p2):
    # weights (tunable)
    w_pattern = 0.35
    w_entropy = 0.20
    w_len = 0.15
    w_unique = 0.15
    w_top = 0.15

    # pattern exact match
    pattern_score = 1.0 if (p1.get("pattern") == p2.get("pattern") and p1.get("pattern") != "UNKNOWN") else 0.0

    # entropy similarity (normalized)
    e1, e2 = safe_float(p1.get("entropy", 0.0)), safe_float(p2.get("entropy", 0.0))
    if max(e1, e2) > 0:
        entropy_sim = 1.0 - abs(e1 - e2) / max(e1, e2)
        entropy_sim = max(0.0, entropy_sim)
    else:
        entropy_sim = 0.0

    # avg length similarity
    a1, a2 = safe_float(p1.get("avg_len", 0.0)), safe_float(p2.get("avg_len", 0.0))
    if max(a1, a2) > 0:
        len_sim = 1.0 - abs(a1 - a2) / max(a1, a2)
        len_sim = max(0.0, len_sim)
    else:
        len_sim = 0.0

    # unique ratio similarity
    u1, u2 = safe_float(p1.get("unique_ratio", 0.0)), safe_float(p2.get("unique_ratio", 0.0))
    unique_sim = 1.0 - abs(u1 - u2)
    unique_sim = max(0.0, min(1.0, unique_sim))

    # top values Jaccard
    top_sim = jaccard_set(p1.get("top_values", []), p2.get("top_values", []))

    base_score = (w_pattern * pattern_score +
                  w_entropy * entropy_sim +
                  w_len * len_sim +
                  w_unique * unique_sim +
                  w_top * top_sim)

    # penalize severe cardinality mismatch:
    low1 = p1.get("is_low_cardinality", False)
    low2 = p2.get("is_low_cardinality", False)
    high1 = p1.get("is_high_cardinality", False)
    high2 = p2.get("is_high_cardinality", False)
    if (low1 and high2) or (low2 and high1):
        # e.g., SEX (low cardinality) vs NAME (high cardinality) -> strong penalty
        base_score *= 0.10

    return max(0.0, min(1.0, base_score))

def final_score(src, tgt, weights=None):
    """
    src, tgt: dicts with keys:
      'col_name','code','profile', optional 'embedding'
    weights: dict with keys name, profile, embedding (if embedding used)
    returns final_score (0..1) and breakdown dict
    """
    p_src = src.get("profile", {})
    p_tgt = tgt.get("profile", {})

    # exact code match
    if src.get("code") == tgt.get("code") and src.get("code") is not None:
        return 1.0, {"reason": "exact_code_match", "name_score": 1.0, "profile_score": 1.0, "embed_score": 1.0}

    # type compatibility check
    compat, reason = type_compatible(p_src, p_tgt)
    if not compat:
        # quick reject: incompatible types
        return 0.0, {"reason": f"type_incompatible:{reason}", "name_score": 0.0, "profile_score": 0.0, "embed_score": 0.0}

    # compute individual pieces
    name_score = name_similarity(src.get("col_name",""), tgt.get("col_name",""))
    profile_score = profile_similarity(p_src, p_tgt)

    embed_score = 0.0
    if src.get("embedding") is not None and tgt.get("embedding") is not None:
        embed_score = cosine_sim(src.get("embedding"), tgt.get("embedding"))

    # set dynamic weights
    if weights is None:
        if embed_score > 0:
            w = {"name": 0.10, "profile": 0.65, "embedding": 0.25}
        else:
            w = {"name": 0.10, "profile": 0.90, "embedding": 0.0}
    else:
        w = weights

    final = w.get("name",0)*name_score + w.get("profile",0)*profile_score + w.get("embedding",0)*embed_score

    breakdown = {
        "reason": "scored",
        "name_score": round(name_score, 4),
        "profile_score": round(profile_score, 4),
        "embed_score": round(embed_score, 4),
        "weight_name": w.get("name",0),
        "weight_profile": w.get("profile",0),
        "weight_embedding": w.get("embedding",0),
        "final_score": round(final, 4)
    }
    return final, breakdown
