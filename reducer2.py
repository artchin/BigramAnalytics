#!/usr/bin/env python3
"""Select top N bigrams by frequency."""

import sys

TOP_N = 10
counter = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    if counter < TOP_N:
        try:
            count, bigram = line.split('\t', 1)
            print(f"{bigram}\t{count}")
            counter += 1
        except ValueError:
            continue
