#!/usr/bin/env python3
"""Aggregate document counts per bigram."""

import sys

current_bigram = None
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        bigram, _ = line.split('\t')
    except ValueError:
        continue
    
    if current_bigram == bigram:
        count += 1
    else:
        if current_bigram is not None:
            print(f"{current_bigram}\t{count}")
        
        current_bigram = bigram
        count = 1

if current_bigram is not None:
    print(f"{current_bigram}\t{count}")
