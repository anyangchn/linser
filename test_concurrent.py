# utf-8
import concurrent.futures
import time

def handle_test(job):
    time.sleep(2)
    print(job)


if __name__ == "__main__":
    jobs = range(1,100)
    futures = []
    result = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for job  in jobs:
            futures.append(executor.submit(handle_test, job))


        for future in concurrent.futures.as_completed(futures):
            result.append(future.result())
