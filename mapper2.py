#!/usr/bin/env python3
"""Invert key-value for numeric sorting."""

import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        bigram, count = line.split('\t', 1)
        print(f"{count}\t{bigram}")
    except ValueError:
        continue
