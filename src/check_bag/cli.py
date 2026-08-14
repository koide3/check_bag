"""Command line interface for check_bag."""

from __future__ import annotations

import argparse
import os
import struct
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
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
        print(f"OK: {mode} {count} messages")


MSGDATA_OP = 0x02

_ros1_state: dict = {}


def _ros1_worker_init(bag: str, deserialize: bool) -> None:
    """Open the bag once per worker process."""
    reader = open_bag(Path(bag))
    reader.open()
    msgtypes = {c.id: c.msgtype for c in reader.connections}
    if deserialize:
        known = reader.typestore.fielddefs
        msgtypes = {cid: t for cid, t in msgtypes.items() if t in known}
    _ros1_state.update(
        reader=reader,
        r1=reader.readers[0],
        msgtypes=msgtypes,
        deserialize=deserialize,
    )


def _scan_chunk(buf: bytes) -> int:
    """Parse all records in a decompressed ROS1 chunk.

    Returns the number of message records; raises on malformed data.
    """
    reader = _ros1_state["reader"]
    msgtypes = _ros1_state["msgtypes"]
    deserialize = _ros1_state["deserialize"]

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


def _ros1_scan_batch(positions: list[int]) -> int:
    """Worker: decompress and scan a batch of chunks, return message count."""
    r1 = _ros1_state["r1"]
    count = 0
    for pos in positions:
        chunk = r1.chunks[pos]
        _ = r1.bio.seek(chunk.datapos)
        data = r1.bio.read(chunk.datasize)
        if len(data) != chunk.datasize:
            msg = f"chunk at offset {pos} is truncated"
            raise ValueError(msg)
        count += _scan_chunk(chunk.decompressor(data))
    return count


def check_ros1_parallel(path: Path, *, deserialize: bool, quiet: bool, max_workers: int) -> None:
    """Check a ROS1 bag by scanning its chunks with a pool of worker processes.

    Raises on any read or deserialization error, and on a mismatch between
    the number of scanned messages and the bag index.
    """
    with open_bag(path) as reader:
        expected = reader.message_count
        positions = sorted(reader.readers[0].chunks)
        if deserialize:
            known = reader.typestore.fielddefs
            unknown = sorted({c.msgtype for c in reader.connections if c.msgtype not in known})
            if unknown and not quiet:
                for msgtype in unknown:
                    print(f"ignoring non-standard type: {msgtype}")

    batchsize = 32
    batches = [positions[i : i + batchsize] for i in range(0, len(positions), batchsize)]
    count = 0
    with ProcessPoolExecutor(
        max_workers=max_workers, initializer=_ros1_worker_init, initargs=(str(path), deserialize)
    ) as executor:
        futures = {executor.submit(_ros1_scan_batch, batch): len(batch) for batch in batches}
        with tqdm(total=len(positions), unit="chunk", disable=quiet) as progress:
            for future in as_completed(futures):
                count += future.result()
                progress.update(futures[future])

    if count != expected:
        msg = f"message count mismatch: scanned {count}, bag index reports {expected}"
        raise ValueError(msg)
    if not quiet:
        mode = "deserialized" if deserialize else "read"
        print(f"OK: {mode} {count} messages")


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
    bag: str, deserialize: bool, info: bool, announce: bool = False
) -> tuple[str, str | None, str | None]:
    """Worker: check a single bag. Returns (bag, error, info_text)."""
    path = Path(bag)
    if announce:
        print(f"checking {bag}", flush=True)
    try:
        with open_bag(path) as reader:
            if info:
                return bag, None, format_info(reader, path)
            check_messages(reader, deserialize=deserialize, quiet=True)
    except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
        return bag, str(exc), None
    return bag, None, None


def probe_bag(bag: str) -> tuple[str, bool, str | None]:
    """Worker: open a bag and report whether it uses chunk compression.

    Returns (bag, is_compressed, error).
    """
    path = Path(bag)
    try:
        with open_bag(path) as reader:
            compressed = False
            if not reader.is2:
                none_decompressor = ros1_decompressors["none"]
                compressed = any(
                    chunk.decompressor is not none_decompressor
                    for chunk in reader.readers[0].chunks.values()
                )
    except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
        return bag, False, str(exc)
    return bag, compressed, None


def check_folder(
    folder: Path,
    *,
    deserialize: bool,
    info: bool,
    quiet: bool,
    max_workers: int,
    max_workers_per_bag: int,
) -> int:
    """Check all bags found under a folder.

    Bags are first probed in parallel to identify compressed ROS1 bags. All
    uncompressed bags are then checked in parallel (one process per bag),
    followed by compressed bags with ``max_workers_per_bag`` processes each,
    running multiple compressed bags concurrently when ``max_workers``
    provides enough workers.
    """
    bags = find_bags(folder)
    if not bags:
        print(f"error: no bags found in {folder}", file=sys.stderr)
        return 1

    failures: list[tuple[str, str]] = []

    if info:
        infos: dict[str, str] = {}
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_bag, str(bag), deserialize, True) for bag in bags]
            with tqdm(total=len(bags), unit="bag", disable=quiet) as progress:
                for future in as_completed(futures):
                    bag, error, info_text = future.result()
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
        if not quiet:
            print(f"checked {len(bags)} bags: {len(bags) - len(failures)} ok, {len(failures)} failed")
        return 1 if failures else 0

    # Stage 1: identify compressed bags by probing bag info in parallel.
    compressed: list[str] = []
    uncompressed: list[str] = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(probe_bag, str(bag)) for bag in bags]
        with tqdm(total=len(bags), unit="bag", desc="probe", disable=quiet) as progress:
            for future in as_completed(futures):
                bag, is_compressed, error = future.result()
                if error is not None:
                    failures.append((bag, error))
                    progress.write(f"FAIL {bag}: {error}", file=sys.stderr)
                elif is_compressed:
                    compressed.append(bag)
                else:
                    uncompressed.append(bag)
                progress.update(1)

    # Stage 2: check uncompressed bags in parallel, one process per bag.
    if uncompressed:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_bag, bag, deserialize, False, not quiet)
                for bag in sorted(uncompressed)
            ]
            with tqdm(total=len(uncompressed), unit="bag", desc="uncompressed", disable=quiet) as progress:
                for future in as_completed(futures):
                    bag, error, _ = future.result()
                    if error is not None:
                        failures.append((bag, error))
                        progress.write(f"FAIL {bag}: {error}", file=sys.stderr)
                    elif not quiet:
                        progress.write(f"OK   {bag}")
                    progress.update(1)

    # Stage 3: check compressed bags with multiple processes per bag, running
    # several bags concurrently when there are enough workers.
    if compressed:
        workers_per_bag = min(max_workers_per_bag, max_workers)
        slots = max(1, max_workers // workers_per_bag)
        if slots == 1:
            for bag in sorted(compressed):
                if not quiet:
                    print(f"checking {bag}")
                try:
                    check_ros1_parallel(
                        Path(bag), deserialize=deserialize, quiet=quiet, max_workers=workers_per_bag
                    )
                except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
                    failures.append((bag, str(exc)))
                    print(f"FAIL {bag}: {exc}", file=sys.stderr)
        else:

            def run_one(bag: str) -> tuple[str, str | None]:
                if not quiet:
                    tqdm.write(f"checking {bag}")
                try:
                    check_ros1_parallel(
                        Path(bag), deserialize=deserialize, quiet=True, max_workers=workers_per_bag
                    )
                except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
                    return bag, str(exc)
                return bag, None

            with ThreadPoolExecutor(max_workers=slots) as pool:
                thread_futures = [pool.submit(run_one, bag) for bag in sorted(compressed)]
                with tqdm(total=len(compressed), unit="bag", desc="compressed", disable=quiet) as progress:
                    for future in as_completed(thread_futures):
                        bag, error = future.result()
                        if error is not None:
                            failures.append((bag, error))
                            progress.write(f"FAIL {bag}: {error}", file=sys.stderr)
                        elif not quiet:
                            progress.write(f"OK   {bag}")
                        progress.update(1)

    if not quiet:
        print(f"checked {len(bags)} bags: {len(bags) - len(failures)} ok, {len(failures)} failed")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="check_bag",
        description="Check the validity of ROS1 and ROS2 bags.",
    )
    parser.add_argument(
        "bag", type=Path, help="path to a ROS1 bag file or ROS2 bag directory (or a folder with --folder)"
    )
    parser.add_argument("--de", action="store_true", help="deserialize messages (non-standard types are ignored)")
    parser.add_argument("--info", action="store_true", help="show meta information of the bag")
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
        help="number of processes used per compressed bag with --folder (default: 8)",
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
            quiet=args.quiet,
            max_workers=args.max_workers,
            max_workers_per_bag=args.max_workers_per_bag,
        )

    try:
        if not args.info and args.bag.is_file() and args.max_workers > 1:
            # ROS1 bags are single files; scan their chunks in parallel.
            check_ros1_parallel(
                args.bag, deserialize=args.de, quiet=args.quiet, max_workers=args.max_workers
            )
            return 0
        with open_bag(args.bag) as reader:
            if args.info:
                print(format_info(reader, args.bag))
            else:
                check_messages(reader, deserialize=args.de, quiet=args.quiet)
    except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
