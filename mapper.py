#!/usr/bin/env python3
"""
Bigram extraction from text documents.
Counts unique bigrams per document (Document Frequency metric).
"""

import sys
import re

current_doc_id = None
doc_bigrams = set()

def clean_text(text):
    text = re.sub(r'[^A-Za-z\s]', '', text)
    text = text.lower()
    return text

def get_bigrams(words):
    bigrams = []
    for i in range(len(words) - 1):
        bigram = f"{words[i]} {words[i+1]}"
        bigrams.append(bigram)
    return bigrams

def flush_bigrams():
    for bigram in doc_bigrams:
        print(f"{bigram}\t1")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t', 1)

    if len(parts) == 2:
        if current_doc_id is not None:
            flush_bigrams()
        
        current_doc_id = parts[0]
        doc_bigrams = set()
        text = parts[1]
    else:
        if current_doc_id is None:
            continue
        text = parts[0]

    cleaned = clean_text(text)    
    words = cleaned.split()
        
    if len(words) < 2:
        continue
        
    bigrams = get_bigrams(words)
    doc_bigrams.update(bigrams)

if current_doc_id is not None:
    flush_bigrams()
