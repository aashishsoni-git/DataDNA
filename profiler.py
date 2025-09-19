# profiler.py
import hashlib
import json
import re
import math
from collections import Counter
import numpy as np

def safe_str(v):
    return "" if v is None else str(v)

def compute_entropy(values):
    if not values:
        return 0.0
    counts = Counter(values)
    total = sum(counts.values())
    probs = [c/total for c in counts.values()]
    return -sum(p * math.log2(p) for p in probs if p > 0)

def guess_pattern(sample_values):
    s = [v for v in sample_values[:50] if v is not None and v != ""]
    if not s:
        return "UNKNOWN"
    # common date patterns
    if all(re.match(r'^\d{4}-\d{2}-\d{2}$', v) for v in s):
        return "DATE_YYYY-MM-DD"
    if all(re.match(r'^\d{2}/\d{2}/\d{4}$', v) for v in s):
        return "DATE_DDMMYYYY"
    if all(re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', v) for v in s):
        return "EMAIL"
    if all(re.match(r'^\d{10,15}$', v) for v in s):
        return "PHONE"
    if all(re.match(r'^\d+(\.\d+)?$', v) for v in s):
        return "NUMERIC"
    # name-like heuristic: many alphabetic values, some spaces, average length
    letters = sum(1 for v in s if re.search(r'[A-Za-z]', v))
    spaces = sum(1 for v in s if ' ' in v)
    avg_len = sum(len(v) for v in s) / len(s)
    if letters/len(s) > 0.6 and spaces/len(s) > 0.15 and avg_len > 4:
        return "NAME"
    # small cardinality -> categorical/enum
    unique = len(set(s))
    if unique <= 20:
        return "CATEGORICAL"
    return "ALPHANUMERIC"

def compute_basic_stats(sample_values):
    sample = [safe_str(v).strip() for v in sample_values]
    n = len(sample)
    non_null = [v for v in sample if v != ""]
    non_null_count = len(non_null)
    pct_null = round(1 - non_null_count / n, 4) if n > 0 else 1.0
    lengths = [len(v) for v in non_null] or [0]
    top = [v for v, _ in Counter(non_null).most_common(10)]
    unique_count = len(set(non_null))
    unique_ratio = round(unique_count / non_null_count, 4) if non_null_count > 0 else 0.0
    entropy = round(compute_entropy(non_null), 4)
    avg_len = round(sum(lengths) / len(lengths), 2) if lengths else 0.0
    min_len = min(lengths) if lengths else 0
    max_len = max(lengths) if lengths else 0
    digits_pct = round(sum(1 for v in non_null if re.fullmatch(r'^\d+$', v)) / (non_null_count or 1), 4)
    alpha_pct  = round(sum(1 for v in non_null if re.search(r'[A-Za-z]', v)) / (non_null_count or 1), 4)
    spaces_pct = round(sum(1 for v in non_null if ' ' in v) / (non_null_count or 1), 4)

    return {
        "sample_count": n,
        "non_null_count": non_null_count,
        "pct_null": pct_null,
        "unique_count": unique_count,
        "unique_ratio": unique_ratio,
        "entropy": entropy,
        "avg_len": avg_len,
        "min_len": min_len,
        "max_len": max_len,
        "top_values": top[:10],
        "digits_pct": digits_pct,
        "alpha_pct": alpha_pct,
        "spaces_pct": spaces_pct
    }

def hash_profile(profile_dict, embedding=None):
    # deterministic serialization
    profile_str = json.dumps(profile_dict, sort_keys=True, ensure_ascii=True)
    if embedding is not None:
        # round floats for stability
        emb_str = ",".join([f"{float(x):.4f}" for x in (embedding[:64] if len(embedding) > 0 else [])])
        profile_str = profile_str + "|" + emb_str
    return hashlib.sha256(profile_str.encode()).hexdigest()

def generate_column_code(values, embedding=None):
    """
    values: list of strings (sample)
    embedding: optional list/np.array (semantic fingerprint)
    returns: (code, profile_dict)
    """
    sample = [safe_str(v) for v in values][:1000]  # cap to avoid huge payload
    profile = compute_basic_stats(sample)
    profile["pattern"] = guess_pattern(sample)
    # low cardinality heuristic
    profile["is_low_cardinality"] = profile["unique_count"] <= 20 or profile["unique_ratio"] < 0.05
    profile["is_high_cardinality"] = profile["unique_ratio"] > 0.5 and profile["unique_count"] > 100
    code = hash_profile(profile, embedding=embedding)
    # include embedding size if provided
    if embedding is not None:
        profile["embedding_len"] = len(embedding)
    return code, profile
