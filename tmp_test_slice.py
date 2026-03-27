MAX_BATCH = 100
jobs = [{"a": 1}, {"b": 2}]
if len(jobs) > MAX_BATCH:
    del jobs[MAX_BATCH:]
print(jobs)
