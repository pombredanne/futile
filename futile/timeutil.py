def parse_time_string(s):
    """
    return given time in seconds

    >>> parse_time_string('1M')
    60
    >>> parse_time_string('1h')
    3600
    >>> parse_time_string('1d1s')
    86401
    """
    bases = {
        'y': 86400 * 365,
        'm': 86400 * 30,
        'w': 86400 * 7,
        'd': 86400,
        'h': 3600,
        'M': 60,
        's': 1,
    }
    secs = 0
    num = 0
    for c in s:
        if c.isdigit():
            num = num * 10 + int(c)
        else:
            secs += bases[c] * num
            num = 0
    return secs


