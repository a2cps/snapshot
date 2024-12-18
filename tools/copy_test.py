import argparse
import asyncio
import shutil
import tempfile
import time
from concurrent import futures
from pathlib import Path


async def copy_async(
    files: list[Path], dst, max_workers: int | None = None
) -> None:
    loop = asyncio.get_running_loop()
    with futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        aws = []
        for file in files:
            aws.append(
                loop.run_in_executor(
                    pool, shutil.copyfile, file, dst / file.name
                )
            )
        await asyncio.gather(*aws)


def copy_sync(files: list[Path], dst) -> None:
    for file in files:
        shutil.copyfile(file, dst / file.name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-files", default=1000, type=int)
    parser.add_argument("--file-size", default=1024 * 1024 * 2, type=int)
    parser.add_argument("--max-workers", default=None)

    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as d:
        tmpdir = Path(d)
        src = tmpdir / "src"
        src.mkdir()
        dst = tmpdir / "dst"
        dst.mkdir()
        files = []
        for i in range(args.n_files):
            f = src / str(i)
            files.append(f)
            f.write_bytes(b"0" * args.file_size)

        s = time.perf_counter()
        asyncio.run(copy_async(files, dst, max_workers=args.max_workers))
        elapsed = time.perf_counter() - s
        print(f"async executed in {elapsed:0.2f} seconds.")

        s = time.perf_counter()
        copy_sync(files, dst)
        elapsed = time.perf_counter() - s
        print(f"sync executed in {elapsed:0.2f} seconds.")
