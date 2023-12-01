from csv import DictWriter
import re
from pathlib import Path
from datetime import datetime

CSV_PATH=f"logs_{datetime.timestamp(datetime.now())}.csv"
LOGS_PATH=Path("ruby/Logs")

LOGIN_REGEX=re.compile(r'^EVENT \[\d+:\d+:\d+.+?\] User (.+?) signed in.$')
JOB_START_REGEX=re.compile(r'^EVENT \[(\d+:\d+:\d+)\.\d{3} INF \].+?Queue entry "(.+?)" started')
JOB_END_REGEX=re.compile(r'^EVENT \[(\d+:\d+:\d+)\.\d{3} INF \].+?Queue entry "(.+?)" finished')

LOG_REGEX=re.compile(r'eventLog\d{8}\.txt')

data = {}
last_user = None
files = [child for child in LOGS_PATH.iterdir() if child.is_file() and LOG_REGEX.match(child.name)] # Find log files

for file in files:
    text = open(file, 'r').readlines()

    for line in text:
        login_matches = LOGIN_REGEX.search(line)
        job_start_matches = JOB_START_REGEX.search(line)
        job_end_matches = JOB_END_REGEX.search(line)

        if login_matches:
            if login_matches.group(1) not in data:
                data[login_matches.group(1)] = {}

            last_user = login_matches.group(1)
        elif job_start_matches:
            if job_start_matches.group(2) not in data[last_user]:
                data[last_user][job_start_matches.group(2)] = {
                    "start": "",
                    "end": ""
                }
            
            data[last_user][job_start_matches.group(2)]["start"] = job_start_matches.group(1)
        elif job_end_matches:
            if job_end_matches.group(2) not in data[last_user]:
                data[last_user][job_end_matches.group(2)] = {
                    "start": "",
                    "end": ""
                }

            data[last_user][job_end_matches.group(2)]["end"] = job_end_matches.group(1)

# Save as CSV
with open(CSV_PATH, 'w', newline='') as csv_file:
    field_names = ["user", "job", "start", "end"]
    csv_writer = DictWriter(csv_file, fieldnames=field_names)

    csv_writer.writeheader()
    for user in data:
        for job in data[user]:
            csv_writer.writerow({
                "user": user, 
                "job": job,
                "start": data[user][job]["start"],
                "end": data[user][job]["end"]
            })
