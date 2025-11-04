#!/usr/bin/env python3
"""Quick fix for backfill.py notes issue."""

import fileinput
import sys

# Fix the issue in backfill.py
with open('scripts/backfill.py', 'r') as f:
    lines = f.readlines()

# Fix line 812
for i, line in enumerate(lines):
    if i == 811 and 'facility[\'verification\'][\'notes\'] +=' in line:
        # Replace the problematic line
        lines[i] = """                notes = facility['verification'].get('notes', '')
                facility['verification']['notes'] = notes + f" | Town enriched: {town or 'null'}" if notes else f"Town enriched: {town or 'null'}"
"""
        break

with open('scripts/backfill.py', 'w') as f:
    f.writelines(lines)

print("Fixed backfill.py")