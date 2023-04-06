#!/usr/bin/env python3
"""Static Assets Importer"""

import os
from pathlib import Path
from time import perf_counter, perf_counter_ns, sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from cloud_uploader import UploaderFactory, MB


## Config params
WORKERS = int(os.environ.get("WORKERS", 4))
RETRY_LIMIT = int(os.environ.get("RETRY_LIMIT", 3))
ERROR_LIMIT = int(os.environ.get("ERROR_LIMIT", 13))
UPLOADER_POOL = list()


def upload_file(source: Path, picker: int):
    started = perf_counter_ns()

    client = UPLOADER_POOL[picker]
    with io.open(source, "rb") as reader:
        client.upload_from_stream(source.name, reader)

    ended = perf_counter_ns()
    return ended - started


def import_asset(source_mpd: str, destination: str):
    mpd = Path(source_mpd)
    assert mpd.is_file(), f"The provided '{source_mpd}' is not a file."

    global UPLOADER_POOL  # TODO: find a better way
    UPLOADER_POOL.extend(UploaderFactory.make(destination) for _ in range(WORKERS))

    queue = [fp for fp in mpd.parent.iterdir() if fp.is_file()]
    total_bytes = sum(fp.stat().st_size for fp in queue)

    copied_count = 0
    tries_count = 0
    error_count = 0

    while queue and tries_count <= RETRY_LIMIT and error_count < ERROR_LIMIT:
        retry = list()
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            task_proxy = {
                executor.submit(upload_file, fp, i % WORKERS): fp
                for i, fp in enumerate(queue)
            }
            for task in as_completed(task_proxy):
                file_path = task_proxy[task]
                try:
                    duration = task.result(timeout=1)
                except Exception as exc:
                    error_count += 1
                    print(f"WARNING: cannot copy {file_path.name}, reason: {exc}.")
                    retry.append(file_path)
                else:
                    copied_count += 1
                    print(
                        f"{file_path.name} copied in {duration // 1_000_000:,} ms.",
                        flush=True,
                    )

                if error_count >= ERROR_LIMIT:
                    print(
                        f"ERROR: Aborted copying '{mpd.parent.name}', "
                        f"too many errors occured ({error_count})."
                    )
                    executor.shutdown(cancel_futures=True)
                    break

                sleep(0.0)
        queue = retry
        tries_count += 1

    return copied_count, tries_count, error_count, total_bytes


def main(source_mpd: str, destination: str):
    assert source_mpd.endswith(
        ".mpd"
    ), f"The provided '{source_mpd}' is not a valid .mpd file"
    started = perf_counter()

    copied, tries, errors, total_bytes = import_asset(source_mpd, destination)

    ended = perf_counter()
    duration = ended - started

    error_rate = errors / (copied or 1)
    bandwidth = total_bytes / (MB * duration)

    print(f"Using {WORKERS} threads, uploaded:")
    print(f"- {copied:,} chunks, in {tries} tries;")
    print(f"- {error_rate * 100:.1f} % error rate (aka {errors} errors);")
    print(f"- or {total_bytes:,} bytes in {duration:.3f} s;")
    print(f"- averaging at {bandwidth:.3f} MB/s ")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <source_mpd> <destination>")
    else:
        main(*sys.argv[1:])
