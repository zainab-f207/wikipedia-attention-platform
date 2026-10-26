import os
import sys
import requests

HEADERS = {"User-Agent": "de-learning-project (github.com/zainab-f207)"}
FOLDER = "data/raw"

day = sys.argv[1]  # for example 2026-10-22
year, month, date = day.split("-")

os.makedirs(FOLDER, exist_ok=True)
complete = 0

for hour in range(24):
    name = f"pageviews-{year}{month}{date}-{hour:02d}0000.gz"
    url = f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/{name}"
    path = os.path.join(FOLDER, name)

    # Ask the server how big the finished file should be
    head = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=30)
    if head.status_code != 200:
        print(name, "is not available (status", head.status_code, ")")
        continue
    expected = int(head.headers["Content-Length"])

    have = 0
    if os.path.exists(path):
        have = os.path.getsize(path)

    if have == expected:
        print(name, "already complete, skipping")
        complete = complete + 1
        continue

    print(name, "downloading", round(expected / 1024 / 1024), "MB")

    attempts = 0
    while have < expected and attempts < 20:
        attempts = attempts + 1
        try:
            # Continue from where the file stopped
            headers = dict(HEADERS)
            headers["Range"] = "bytes=" + str(have) + "-"
            response = requests.get(url, headers=headers, stream=True, timeout=60)

            if response.status_code == 206:
                mode = "ab"      # server resumed: add to the file
            else:
                mode = "wb"      # server sent everything: start over

            with open(path, mode) as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
        except requests.exceptions.RequestException as error:
            print("  connection problem:", type(error).__name__, "- trying again")

        have = os.path.getsize(path)

    if have == expected:
        print("  done")
        complete = complete + 1
    else:
        print("  INCOMPLETE:", have, "of", expected, "bytes")

print("Complete files:", complete, "of 24")
