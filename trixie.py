#!/usr/bin/env python3
import os
from pathlib import Path
from time import perf_counter_ns, sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from cloud_uploader import UploaderFactory


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

    queue = (fp for fp in mpd.parent.iterdir() if fp.is_file())

    copied_count = 0
    tries_count = 0
    error_count = 0
    linear_duration = 0

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
                    linear_duration += duration
                except Exception as exc:
                    error_count += 1
                    print(f"WARNING: cannot copy {file_path.name}, reason: {exc}.")
                    retry.append(file_path)
                else:
                    copied_count += 1
                    print(f"{file_path.name} copied in {duration // 1_000_000:,} ms.", flush=True)

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

    return copied_count, tries_count, error_count, linear_duration


def main(source_mpd: str, destination: str):
    assert source_mpd.endswith(
        ".mpd"
    ), f"The provided '{source_mpd}' is not a valid .mpd file"
    started = perf_counter_ns()

    copied, tries, errors, linear_duration_ns = import_asset(source_mpd, destination)

    ended = perf_counter_ns()
    duration_ns = ended - started

    linear_duration = linear_duration_ns / 1_000_000_000
    duration = duration_ns / 1_000_000_000
    print(
        f"Uploaded {copied:,} chunks, in {tries} tries, "
        f"with {errors} errors ({errors * 100 / copied:.1f} %)."
    )
    print(f"and it took {duration:.3f} s instead of {linear_duration:.3f} s.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <source_mpd> <destination>")
    else:
        main(*sys.argv[1:])
