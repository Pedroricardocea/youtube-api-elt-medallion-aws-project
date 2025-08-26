import os, time, boto3

glue = boto3.client("glue")

CRAWLER = os.environ["CSV_CLEAN_CRAWLER_NAME"]
JOB = os.environ["ANALYTICS_JOB_RUN"]

# tweakable 
POLL_SECS = 15  # how often to check
TIMEOUT_SECS = 20 * 60  # max wait 20 min


def wait_until_ready(name: str):
    start = time.time()
    while True:
        state = glue.get_crawler(Name=name)["Crawler"]["State"]
        print(f"[crawler:{name}] state={state}")
        if state == "READY":
            return
        if time.time() - start > TIMEOUT_SECS:
            raise TimeoutError(f"Crawler {name} not READY after {TIMEOUT_SECS}s")
        time.sleep(POLL_SECS)


def lambda_handler(event, context):
    # 1) Try to start; ignore if it's already running
    try:
        glue.start_crawler(Name=CRAWLER)
        print(f"Started crawler: {CRAWLER}")
    except glue.exceptions.CrawlerRunningException:
        print(f"Crawler {CRAWLER} already running; will wait.")

    # 2) Wait for completion
    wait_until_ready(CRAWLER)

    # 3) Start the Glue job
    resp = glue.start_job_run(JobName=JOB)
    job_run_id = resp["JobRunId"]
    print(f"Started Glue job {JOB}, runId={job_run_id}")

    return {"crawler": CRAWLER, "job": JOB, "jobRunId": job_run_id}
