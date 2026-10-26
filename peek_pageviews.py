import gzip
import heapq
import sys

filename = sys.argv[1]

lines = 0
bad_lines = 0
top = []  # the 5 biggest rows so far

with gzip.open(filename, "rt", encoding="utf-8", errors="replace") as f:
    for line in f:
        lines = lines + 1
        if lines <= 5:
            print("First lines:", line.strip())

        parts = line.split(" ")
        if len(parts) != 4:
            bad_lines = bad_lines + 1
            continue

        views = int(parts[2])
        heapq.heappush(top, (views, parts[0], parts[1]))
        if len(top) > 5:
            heapq.heappop(top)

print("Total lines:", lines)
print("Lines without 4 fields:", bad_lines)
print("Top 5 (views, domain, page):")
for row in sorted(top, reverse=True):
    print(row)
