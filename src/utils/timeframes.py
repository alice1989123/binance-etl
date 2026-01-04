def interval_to_ms(interval: str) -> int:
    unit = interval[-1]
    n = int(interval[:-1])
    if unit == "m": return n * 60_000
    if unit == "h": return n * 3_600_000
    if unit == "d": return n * 86_400_000
    if unit == "w": return n * 7 * 86_400_000
    if unit == "M": return n * 30 * 86_400_000
    raise ValueError(f"Unsupported interval: {interval}")
