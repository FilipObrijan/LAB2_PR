import sys
import time
import urllib.request
import urllib.error
import threading
from typing import Tuple, List


def make_request(url: str, request_id: int, results: List, lock: threading.Lock) -> None:
    start_time = time.time()
    try:
        with urllib.request.urlopen(url, timeout=120) as response:
            # read the response to ensure full request completion
            _ = response.read()
            duration = time.time() - start_time
            status = response.status
            result = (request_id, duration, True, f"HTTP {status}")
    except urllib.error.HTTPError as e:
        duration = time.time() - start_time
        result = (request_id, duration, False, f"HTTP {e.code} Error")
    except urllib.error.URLError as e:
        duration = time.time() - start_time
        result = (request_id, duration, False, f"URL Error: {e.reason}")
    except Exception as e:
        duration = time.time() - start_time
        result = (request_id, duration, False, f"Error: {str(e)}")
    
    with lock:
        results.append(result)


def run_concurrent_test(url: str, num_requests: int, delay_between: float = 0) -> None:
    print(f"\n{'=' * 70}")
    print(f"Testing URL: {url}")
    print(f"Number of concurrent requests: {num_requests}")
    if delay_between > 0:
        print(f"Request submission delay: {delay_between:.3f}s (rate: {1 / delay_between:.2f} req/s)")
    print(f"{'=' * 70}\n")

    results: List[Tuple[int, float, bool, str]] = []
    results_lock = threading.Lock()
    threads = []

    # start timing
    overall_start = time.time()

    # Create a thread for each request
    for i in range(1, num_requests + 1):
        thread = threading.Thread(
            target=make_request,
            args=(url, i, results, results_lock),
            daemon=True
        )
        threads.append(thread)
        thread.start()
        
        # Optional delay between thread creation
        if delay_between > 0 and i < num_requests:
            time.sleep(delay_between)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    overall_duration = time.time() - overall_start

    # calculate statistics
    successful_requests = [r for r in results if r[2]]
    failed_requests = [r for r in results if not r[2]]

    # calculate successful requests per second
    successful_req_per_sec = len(successful_requests) / overall_duration if overall_duration > 0 else 0

    # calculate average response time for successful requests
    avg_response_time = sum(r[1] for r in successful_requests) / len(successful_requests) if successful_requests else 0

    # print summary
    print(f"\n{'=' * 70}")
    print(f"RESULTS SUMMARY")
    print(f"{'=' * 70}")
    print(f"Total requests:        {num_requests}")
    print(f"Successful:            {len(successful_requests)}")
    print(f"Failed:                {len(failed_requests)}")
    print(f"Total time:            {overall_duration:.3f}s")
    print(f"Successful req/sec:    {successful_req_per_sec:.2f}")
    print(f"{'=' * 70}")

    # show breakdown of failed requests if any
    if failed_requests:
        print(f"\nFailed Request Details:")
        error_counts = {}
        for req_id, duration, success, status in failed_requests:
            error_counts[status] = error_counts.get(status, 0) + 1
        for error_type, count in sorted(error_counts.items()):
            print(f"  {error_type}: {count}")
    print()


def main():
    if len(sys.argv) < 5 or len(sys.argv) > 6:
        print("Usage: python3 request_test.py <ip> <port> <path> <nr_req> [delay]")
        print("\nExamples:")
        print("  python3 request_test.py 127.0.0.1 8001 public/index.html 100")
        print("  python3 request_test.py localhost 8001 public/index.html 100 0.25")
        sys.exit(1)

    ip = sys.argv[1]
    port = sys.argv[2]
    path = sys.argv[3]

    try:
        num_requests = int(sys.argv[4])
        if num_requests < 1:
            raise ValueError("Number of requests must be positive")
    except ValueError as e:
        print(f"Error: Invalid number of requests: {e}")
        sys.exit(1)

    # Optional delay parameter
    delay_between = 0
    if len(sys.argv) == 6:
        try:
            delay_between = float(sys.argv[5])
            if delay_between < 0:
                raise ValueError("Delay must be non-negative")
        except ValueError as e:
            print(f"Error: Invalid delay value: {e}")
            sys.exit(1)

    # Ensure path starts with /
    if not path.startswith('/'):
        path = '/' + path

    # Construct URL
    url = f"http://{ip}:{port}{path}"

    try:
        run_concurrent_test(url, num_requests, delay_between)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\nError during test: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()