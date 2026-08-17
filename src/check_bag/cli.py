"""Command line interface for check_bag."""

from __future__ import annotations

import argparse
import os
import struct
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from rosbags.highlevel import AnyReader
from rosbags.rosbag1.reader import decompressors as ros1_decompressors
from rosbags.typesys import Stores, get_typestore
from tqdm import tqdm


def human_size(num: int) -> str:
    """Format a byte count as a human readable string."""
    size = float(num)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024.0 or unit == "TiB":
            break
        size /= 1024.0
    if unit == "B":
        return f"{num} B"
    return f"{size:.1f} {unit}"


def bag_size(path: Path) -> int:
    """Return the total size of a bag file (ROS1) or bag directory (ROS2)."""
    if path.is_dir():
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    return path.stat().st_size


def open_bag(path: Path) -> AnyReader:
    """Open a bag with a fallback typestore for bags without embedded type
    definitions; definitions stored in the bag still take precedence."""
    return AnyReader([path], default_typestore=get_typestore(Stores.LATEST))


def format_info(reader: AnyReader, path: Path) -> str:
    """Return meta information of the bag as a printable string."""
    size = bag_size(path)
    duration = reader.duration / 1e9
    lines = [
        f"path:     {path}",
        f"type:     {'ROS2' if reader.is2 else 'ROS1'}",
        f"size:     {human_size(size)} ({size} bytes)",
        f"duration: {duration:.3f} s",
        f"messages: {reader.message_count}",
        f"topics:   {len(reader.topics)}",
    ]
    width = max((len(name) for name in reader.topics), default=0)
    for name, topic in sorted(reader.topics.items()):
        lines.append(f"  {name:<{width}}  {topic.msgtype}  ({topic.msgcount} msgs)")
    return "\n".join(lines)


def check_messages(reader: AnyReader, *, deserialize: bool, quiet: bool) -> None:
    """Read all messages in the bag, optionally deserializing them.

    Raises on any read or deserialization error.
    """
    start = time.perf_counter()
    connections = list(reader.connections)

    if deserialize:
        known = reader.typestore.fielddefs
        unknown = sorted({c.msgtype for c in connections if c.msgtype not in known})
        connections = [c for c in connections if c.msgtype in known]
        if unknown and not quiet:
            for msgtype in unknown:
                print(f"ignoring non-standard type: {msgtype}")

    count = 0
    if connections:
        total = sum(c.msgcount for c in connections)
        with tqdm(total=total, unit="msg", disable=quiet) as progress:
            for connection, _timestamp, rawdata in reader.messages(connections=connections):
                if deserialize:
                    reader.deserialize(rawdata, connection.msgtype)
                count += 1
                progress.update(1)

    if not quiet:
        mode = "deserialized" if deserialize else "read"
        print(f"OK: {mode} {count} messages ({time.perf_counter() - start:.1f}s)")


def check_quick(reader: AnyReader, *, deserialize: bool, quiet: bool) -> None:
    """Check that the bag opens and its first and last messages are valid.

    Raises on any read or deserialization error.
    """
    start = time.perf_counter()
    if reader.message_count == 0:
        if not quiet:
            print(f"OK: quick check, bag opened (no messages) ({time.perf_counter() - start:.1f}s)")
        return

    first = next(reader.messages(), None)
    if first is None:
        msg = "bag index reports messages but none could be read"
        raise ValueError(msg)
    last = None
    for entry in reader.messages(start=reader.end_time - 1):
        last = entry
    if last is None:
        msg = "could not read the last message"
        raise ValueError(msg)

    if deserialize:
        known = reader.typestore.fielddefs
        for connection, _timestamp, rawdata in (first, last):
            if connection.msgtype in known:
                _ = reader.deserialize(rawdata, connection.msgtype)

    if not quiet:
        mode = "valid" if not deserialize else "deserialized"
        print(
            f"OK: quick check, first and last of {reader.message_count} messages {mode}"
            f" ({time.perf_counter() - start:.1f}s)"
        )


MSGDATA_OP = 0x02
CHUNK_OP = 0x05

# Per worker-process caches of open bag file handles and readers, so one
# process pool can scan chunks from many bags without reopening them.
_worker_files: dict[str, object] = {}
_worker_readers: dict[str, tuple] = {}


def _scan_chunk(buf: bytes, reader: AnyReader | None, msgtypes: dict[int, str], deserialize: bool) -> int:
    """Parse all records in a decompressed ROS1 chunk.

    Returns the number of message records; raises on malformed data.
    """
    pos = 0
    end = len(buf)
    count = 0
    while pos < end:
        (hlen,) = struct.unpack_from("<I", buf, pos)
        pos += 4
        hend = pos + hlen
        op = None
        conn = None
        while pos < hend:
            (flen,) = struct.unpack_from("<I", buf, pos)
            pos += 4
            fend = pos + flen
            eq = buf.index(b"=", pos, fend)
            name = buf[pos:eq]
            if name == b"op":
                op = buf[eq + 1]
            elif name == b"conn":
                (conn,) = struct.unpack_from("<I", buf, eq + 1)
            pos = fend
        if pos != hend:
            msg = "malformed record header in chunk"
            raise ValueError(msg)
        (dlen,) = struct.unpack_from("<I", buf, pos)
        pos += 4
        if pos + dlen > end:
            msg = "record data exceeds chunk size"
            raise ValueError(msg)
        if op == MSGDATA_OP:
            count += 1
            if deserialize:
                msgtype = msgtypes.get(conn)
                if msgtype is not None:
                    reader.deserialize(buf[pos : pos + dlen], msgtype)
        pos += dlen
    return count


def scan_ros1_batch(bag: str, deserialize: bool, chunks: list[tuple[int, int, str]]) -> int:
    """Worker: decompress and scan a batch of chunks, return message count.

    Each chunk is described as (datapos, datasize, compression). Without
    deserialization only a raw file handle is opened; with it, the full
    reader is opened once per worker to build the typestore.
    """
    reader = None
    msgtypes: dict[int, str] = {}
    if deserialize:
        entry = _worker_readers.get(bag)
        if entry is None:
            opened = open_bag(Path(bag))
            opened.open()
            known = opened.typestore.fielddefs
            entry = (opened, {c.id: c.msgtype for c in opened.connections if c.msgtype in known})
            _worker_readers[bag] = entry
        reader, msgtypes = entry
        bio = reader.readers[0].bio
    else:
        bio = _worker_files.get(bag)
        if bio is None:
            bio = open(bag, "rb")  # noqa: SIM115 - closed on process exit
            _worker_files[bag] = bio
    count = 0
    for datapos, datasize, compression in chunks:
        _ = bio.seek(datapos)
        data = bio.read(datasize)
        if len(data) != datasize:
            msg = f"chunk at offset {datapos} is truncated"
            raise ValueError(msg)
        count += _scan_chunk(ros1_decompressors[compression](data), reader, msgtypes, deserialize)
    return count


def check_ros1_parallel(path: Path, *, deserialize: bool, quiet: bool, max_workers: int) -> None:
    """Check a ROS1 bag by scanning its chunks with a pool of worker processes.

    Raises on any read or deserialization error, and on a mismatch between
    the number of scanned messages and the bag index.
    """
    start = time.perf_counter()
    compression_names = {id(func): name for name, func in ros1_decompressors.items()}
    with open_bag(path) as reader:
        expected = reader.message_count
        chunks = [
            (chunk.datapos, chunk.datasize, compression_names[id(chunk.decompressor)])
            for _, chunk in sorted(reader.readers[0].chunks.items())
        ]
        if deserialize:
            known = reader.typestore.fielddefs
            unknown = sorted({c.msgtype for c in reader.connections if c.msgtype not in known})
            if unknown and not quiet:
                for msgtype in unknown:
                    print(f"ignoring non-standard type: {msgtype}")

    batchsize = 32
    batches = [chunks[i : i + batchsize] for i in range(0, len(chunks), batchsize)]
    count = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(scan_ros1_batch, str(path), deserialize, batch): len(batch)
            for batch in batches
        }
        with tqdm(total=len(chunks), unit="chunk", disable=quiet) as progress:
            for future in as_completed(futures):
                count += future.result()
                progress.update(futures[future])

    if count != expected:
        msg = f"message count mismatch: scanned {count}, bag index reports {expected}"
        raise ValueError(msg)
    if not quiet:
        mode = "deserialized" if deserialize else "read"
        print(f"OK: {mode} {count} messages ({time.perf_counter() - start:.1f}s)")


def ros1_compression(path: Path) -> str:
    """Peek at the first chunk record of a ROS1 bag to get its compression.

    Only reads the head of the file; returns 'none' when undeterminable.
    """
    try:
        with path.open("rb") as bagfile:
            _ = bagfile.readline()  # version line, e.g. '#ROSBAG V2.0'
            while True:
                head = bagfile.read(4)
                if len(head) < 4:
                    return "none"
                (hlen,) = struct.unpack("<I", head)
                header = bagfile.read(hlen)
                op = None
                compression = None
                idx = 0
                while idx < hlen:
                    (flen,) = struct.unpack_from("<I", header, idx)
                    idx += 4
                    fend = idx + flen
                    eq = header.index(b"=", idx, fend)
                    name = header[idx:eq]
                    if name == b"op":
                        op = header[eq + 1]
                    elif name == b"compression":
                        compression = header[eq + 1 : fend].decode()
                    idx = fend
                (dlen,) = struct.unpack("<I", bagfile.read(4))
                if op == CHUNK_OP:
                    return compression or "none"
                _ = bagfile.seek(dlen, os.SEEK_CUR)
    except Exception:  # noqa: BLE001 - only used for scheduling, not validity
        return "none"


def find_bags(folder: Path) -> list[Path]:
    """Find all ROS1 bag files and ROS2 bag directories under a folder."""
    bags: list[Path] = []
    for root, dirs, files in os.walk(folder):
        rootpath = Path(root)
        if "metadata.yaml" in files:
            bags.append(rootpath)
            dirs.clear()  # do not descend into a ROS2 bag directory
            continue
        bags.extend(rootpath / name for name in files if name.endswith(".bag"))
    return sorted(bags)


def process_bag(
    bag: str, deserialize: bool, info: bool, announce: bool = False, quick: bool = False
) -> tuple[str, str | None, str | None, float]:
    """Worker: check a single bag. Returns (bag, error, info_text, elapsed)."""
    path = Path(bag)
    if announce:
        print(f"checking {bag}", flush=True)
    start = time.perf_counter()
    try:
        with open_bag(path) as reader:
            if info:
                return bag, None, format_info(reader, path), time.perf_counter() - start
            if quick:
                check_quick(reader, deserialize=deserialize, quiet=True)
            else:
                check_messages(reader, deserialize=deserialize, quiet=True)
    except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
        return bag, str(exc), None, time.perf_counter() - start
    return bag, None, None, time.perf_counter() - start


def check_folder(
    folder: Path,
    *,
    deserialize: bool,
    info: bool,
    quick: bool,
    quiet: bool,
    max_workers: int,
) -> int:
    """Check all bags found under a folder.

    All ROS1 bags share one global pool of ``max_workers`` processes that
    scans their chunks (heavy bags queued first: compressed before
    uncompressed, larger before smaller), so every worker stays busy until
    the whole queue drains. ROS2 bags each run as a single-process task in
    the same pool and are submitted first so they overlap the chunk work.
    With ``quick``, every bag instead runs as a light single-process task
    that only validates opening and the first and last messages.
    """
    start = time.perf_counter()
    bags = find_bags(folder)
    if not bags:
        print(f"error: no bags found in {folder}", file=sys.stderr)
        return 1

    failures: list[tuple[str, str]] = []

    def summary() -> None:
        total = time.perf_counter() - start
        if failures:
            print(f"failed bags ({len(failures)}):", file=sys.stderr)
            for bag, error in sorted(failures):
                print(f"  {bag}: {error}", file=sys.stderr)
        if not quiet:
            print(
                f"checked {len(bags)} bags: {len(bags) - len(failures)} ok,"
                f" {len(failures)} failed in {total:.1f}s"
            )

    if info:
        infos: dict[str, str] = {}
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_bag, str(bag), deserialize, True) for bag in bags]
            with tqdm(total=len(bags), unit="bag", disable=quiet) as progress:
                for future in as_completed(futures):
                    bag, error, info_text, _elapsed = future.result()
                    if error is not None:
                        failures.append((bag, error))
                        progress.write(f"FAIL {bag}: {error}", file=sys.stderr)
                    else:
                        infos[bag] = info_text
                    progress.update(1)
        for bag in bags:
            if str(bag) in infos:
                print(infos[str(bag)])
                print()
        summary()
        return 1 if failures else 0

    if quick:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            quick_futures = [
                executor.submit(process_bag, str(bag), deserialize, False, not quiet, True)
                for bag in bags
            ]
            with tqdm(total=len(bags), unit="bag", disable=quiet) as progress:
                for future in as_completed(quick_futures):
                    bag, error, _, elapsed = future.result()
                    if error is not None:
                        failures.append((bag, error))
                        progress.write(f"FAIL {bag}: {error} ({elapsed:.1f}s)", file=sys.stderr)
                    elif not quiet:
                        progress.write(f"OK   {bag} ({elapsed:.1f}s)")
                    progress.update(1)
        summary()
        return 1 if failures else 0

    # Queue ROS2 bags first (single-process tasks that overlap the chunk
    # work), then ROS1 bags heavy-first: compressed before uncompressed,
    # larger before smaller.
    ros1_paths = [bag for bag in bags if bag.is_file()]
    ros2_paths = sorted((bag for bag in bags if bag.is_dir()), key=bag_size, reverse=True)
    heavy = [path for path in ros1_paths if ros1_compression(path) != "none"]
    light = [path for path in ros1_paths if path not in set(heavy)]
    ros1_ordered = sorted(heavy, key=bag_size, reverse=True) + sorted(light, key=bag_size, reverse=True)

    batchsize = 32
    compression_names = {id(func): name for name, func in ros1_decompressors.items()}
    futures: dict = {}  # future -> (kind, bag, nchunks)
    bag_state: dict[str, dict] = {}

    def finish_bag(bag: str, progress: tqdm) -> None:
        state = bag_state[bag]
        elapsed = time.perf_counter() - state["start"]
        error = state["error"]
        if error is None and state["count"] != state["expected"]:
            error = (
                f"message count mismatch: scanned {state['count']},"
                f" bag index reports {state['expected']}"
            )
        if error is not None:
            failures.append((bag, error))
            progress.write(f"FAIL {bag}: {error} ({elapsed:.1f}s)", file=sys.stderr)
        elif not quiet:
            progress.write(f"OK   {bag} ({elapsed:.1f}s)")

    with (
        ProcessPoolExecutor(max_workers=max_workers) as executor,
        tqdm(total=0, unit="chunk", disable=quiet) as progress,
    ):
        for path in ros2_paths:
            future = executor.submit(process_bag, str(path), deserialize, False, not quiet)
            futures[future] = ("ros2", str(path), 0)

        for path in ros1_ordered:
            bag = str(path)
            if not quiet:
                tqdm.write(f"checking {bag}")
            state = {"pending": 0, "count": 0, "expected": 0, "error": None,
                     "start": time.perf_counter(), "futures": []}
            bag_state[bag] = state
            try:
                with open_bag(path) as reader:
                    state["expected"] = reader.message_count
                    chunks = [
                        (chunk.datapos, chunk.datasize, compression_names[id(chunk.decompressor)])
                        for _, chunk in sorted(reader.readers[0].chunks.items())
                    ]
                    if deserialize:
                        known = reader.typestore.fielddefs
                        unknown = sorted(
                            {c.msgtype for c in reader.connections if c.msgtype not in known}
                        )
                        if unknown and not quiet:
                            for msgtype in unknown:
                                tqdm.write(f"ignoring non-standard type: {msgtype} ({bag})")
            except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
                state["error"] = str(exc)
                finish_bag(bag, progress)
                continue
            batches = [chunks[i : i + batchsize] for i in range(0, len(chunks), batchsize)]
            state["pending"] = len(batches)
            for batch in batches:
                future = executor.submit(scan_ros1_batch, bag, deserialize, batch)
                futures[future] = ("ros1", bag, len(batch))
                state["futures"].append(future)
            progress.total += len(chunks)
            progress.refresh()
            if not batches:
                finish_bag(bag, progress)

        for future in as_completed(futures):
            kind, bag, nchunks = futures[future]
            if kind == "ros2":
                _, error, _, elapsed = future.result()
                if error is not None:
                    failures.append((bag, error))
                    progress.write(f"FAIL {bag}: {error} ({elapsed:.1f}s)", file=sys.stderr)
                elif not quiet:
                    progress.write(f"OK   {bag} ({elapsed:.1f}s)")
                continue
            state = bag_state[bag]
            state["pending"] -= 1
            if not future.cancelled():
                try:
                    state["count"] += future.result()
                except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
                    if state["error"] is None:
                        state["error"] = str(exc)
                        for sibling in state["futures"]:
                            _ = sibling.cancel()
            progress.update(nchunks)
            if state["pending"] == 0:
                finish_bag(bag, progress)

    summary()
    return 1 if failures else 0


def main() -> int:
    # Line-buffer stdout even when piped, so forked worker processes never
    # inherit (and re-flush) buffered output lines.
    if sys.stdout is not None and hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(
        prog="check_bag",
        description="Check the validity of ROS1 and ROS2 bags.",
    )
    parser.add_argument(
        "bag", type=Path, help="path to a ROS1 bag file or ROS2 bag directory (or a folder with --folder)"
    )
    parser.add_argument("--de", action="store_true", help="deserialize messages (non-standard types are ignored)")
    parser.add_argument("--info", action="store_true", help="show meta information of the bag")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="check only that the bag opens and its first and last messages are valid",
    )
    parser.add_argument("--quiet", action="store_true", help="do not show progress")
    parser.add_argument(
        "--folder", action="store_true", help="check all ROS1 and ROS2 bags found under the specified directory"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="number of processes used for --folder and for ROS1 chunk scanning (default: 8)",
    )
    parser.add_argument(
        "--max-workers-per-bag",
        type=int,
        default=8,
        help="deprecated and ignored: --folder now schedules all chunks in one global worker pool",
    )
    args = parser.parse_args()

    if not args.bag.exists():
        print(f"error: no such file or directory: {args.bag}", file=sys.stderr)
        return 1

    if args.folder:
        if not args.bag.is_dir():
            print(f"error: not a directory: {args.bag}", file=sys.stderr)
            return 1
        return check_folder(
            args.bag,
            deserialize=args.de,
            info=args.info,
            quick=args.quick,
            quiet=args.quiet,
            max_workers=args.max_workers,
        )

    try:
        if not args.info and not args.quick and args.bag.is_file() and args.max_workers > 1:
            # ROS1 bags are single files; scan their chunks in parallel.
            check_ros1_parallel(
                args.bag, deserialize=args.de, quiet=args.quiet, max_workers=args.max_workers
            )
            return 0
        with open_bag(args.bag) as reader:
            if args.info:
                print(format_info(reader, args.bag))
            elif args.quick:
                check_quick(reader, deserialize=args.de, quiet=args.quiet)
            else:
                check_messages(reader, deserialize=args.de, quiet=args.quiet)
    except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
