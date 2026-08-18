"""Convert compressed ROS1 bags into uncompressed ROS1 bags."""

from __future__ import annotations

import argparse
import struct
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from rosbags.rosbag1 import Reader, Writer
from rosbags.rosbag1.reader import decompressors as ros1_decompressors
from tqdm import tqdm

from check_bag.cli import MSGDATA_OP, is_camera_msgtype, ros1_compression

ORIG_SUFFIX = ".orig.bag"
TMP_SUFFIX = ".decompress-tmp.bag"
ROS1_MAGIC = b"#ROSBAG V2.0"


def orig_path(bag: Path) -> Path:
    """Return the path the original bag is kept at."""
    return bag.with_suffix(ORIG_SUFFIX)


def find_bags(folder: Path) -> list[Path]:
    """Recursively find ROS1 bags, ignoring kept originals and temporaries."""
    return sorted(
        path
        for path in folder.rglob("*.bag")
        if path.is_file() and not path.name.endswith((ORIG_SUFFIX, TMP_SUFFIX))
    )


def parse_chunk_messages(buf: bytes, keep: frozenset[int]) -> list[tuple[int, int, int, bytes]]:
    """Extract (timestamp, offset, connection id, data) of a chunk's messages.

    Only messages of the kept connections are returned. Raises on malformed
    data, so a damaged bag never converts silently.
    """
    out = []
    pos = 0
    end = len(buf)
    while pos < end:
        record_start = pos
        (hlen,) = struct.unpack_from("<I", buf, pos)
        pos += 4
        hend = pos + hlen
        op = None
        conn = None
        stamp = None
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
            elif name == b"time":
                sec, nsec = struct.unpack_from("<II", buf, eq + 1)
                stamp = sec * 10**9 + nsec
            pos = fend
        if pos != hend:
            msg = "malformed record header in chunk"
            raise ValueError(msg)
        (dlen,) = struct.unpack_from("<I", buf, pos)
        pos += 4
        if pos + dlen > end:
            msg = "record data exceeds chunk size"
            raise ValueError(msg)
        if op == MSGDATA_OP and conn in keep:
            if stamp is None:
                msg = "message record without timestamp"
                raise ValueError(msg)
            out.append((stamp, record_start, conn, buf[pos : pos + dlen]))
        pos += dlen
    return out


_worker_files: dict[str, object] = {}


def decompress_chunk_batch(
    bag: str, batch: list[tuple[int, int, int, str]], keep: frozenset[int]
) -> list[tuple[int, int, int, int, bytes]]:
    """Worker: decompress chunks and return their messages.

    Each chunk is (sequence, datapos, datasize, compression); each message
    is (timestamp, chunk sequence, offset, connection id, data).
    """
    bio = _worker_files.get(bag)
    if bio is None:
        bio = open(bag, "rb")  # noqa: SIM115 - closed on process exit
        _worker_files[bag] = bio
    out = []
    for seq, datapos, datasize, compression in batch:
        _ = bio.seek(datapos)
        data = bio.read(datasize)
        if len(data) != datasize:
            msg = f"chunk at offset {datapos} is truncated"
            raise ValueError(msg)
        raw = ros1_decompressors[compression](data)
        out.extend(
            (stamp, seq, offset, conn, payload)
            for stamp, offset, conn, payload in parse_chunk_messages(raw, keep)
        )
    return out


def copy_uncompressed_parallel(
    src: Path, dst: Path, max_workers: int, exclude_camera: bool = False
) -> int:
    """Decompress a bag with several processes, then write it in one go.

    All messages are held in memory until the whole bag is decompressed, so
    this needs roughly as much free memory as the uncompressed bag size.
    """
    compression_names = {id(func): name for name, func in ros1_decompressors.items()}
    with Reader(src) as reader:
        connections = list(reader.connections)
        if exclude_camera:
            connections = [c for c in connections if not is_camera_msgtype(c.msgtype)]
        keep = frozenset(c.id for c in connections)
        chunks = [
            (seq, chunk.datapos, chunk.datasize, compression_names[id(chunk.decompressor)])
            for seq, (_, chunk) in enumerate(sorted(reader.chunks.items()))
        ]
        conn_meta = [
            (c.id, c.topic, c.msgtype, c.msgdef.data, c.digest, c.ext.callerid, c.ext.latching)
            for c in connections
        ]

    batchsize = 8
    batches = [chunks[i : i + batchsize] for i in range(0, len(chunks), batchsize)]
    messages: list[tuple[int, int, int, int, bytes]] = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(decompress_chunk_batch, str(src), batch, keep) for batch in batches
        ]
        for future in as_completed(futures):
            messages.extend(future.result())

    # Restore the reader's ordering: by timestamp, then chunk, then offset.
    messages.sort(key=lambda entry: entry[:3])

    # Writer defaults to no compression, which is exactly what we want.
    with Writer(dst) as writer:
        connmap = {
            cid: writer.add_connection(
                topic, msgtype, msgdef=msgdef, md5sum=digest, callerid=callerid, latching=latching
            )
            for cid, topic, msgtype, msgdef, digest, callerid, latching in conn_meta
        }
        for stamp, _seq, _offset, conn, data in messages:
            writer.write(connmap[conn], stamp, data)
    return len(messages)


def copy_uncompressed(src: Path, dst: Path, exclude_camera: bool = False) -> int:
    """Write an uncompressed copy of a ROS1 bag, returning the message count."""
    # Writer defaults to no compression, which is exactly what we want.
    with Reader(src) as reader, Writer(dst) as writer:
        connections = list(reader.connections)
        if exclude_camera:
            connections = [c for c in connections if not is_camera_msgtype(c.msgtype)]
        connmap = {}
        for connection in connections:
            connmap[connection.id] = writer.add_connection(
                connection.topic,
                connection.msgtype,
                msgdef=connection.msgdef.data,
                md5sum=connection.digest,
                callerid=connection.ext.callerid,
                latching=connection.ext.latching,
            )
        count = 0
        for connection, timestamp, data in reader.messages(connections=connections):
            writer.write(connmap[connection.id], timestamp, data)
            count += 1
    return count


def available_memory() -> int | None:
    """Return MemAvailable in bytes, or None when it cannot be determined."""
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except Exception:  # noqa: BLE001 - only used for an advisory warning
        return None
    return None


def decompress_bag(
    bag: str,
    force: bool = False,
    exclude_camera: bool = False,
    super_parallel: bool = False,
    max_workers: int = 8,
) -> tuple[str, str, str | None, float]:
    """Worker: convert one bag in place, keeping the original as *.orig.bag.

    Returns (bag, status, detail, elapsed) with status one of
    'converted', 'skipped', or 'failed'.
    """
    start = time.perf_counter()
    path = Path(bag)
    orig = orig_path(path)
    tmp = path.with_suffix(TMP_SUFFIX)

    if orig.exists() and not force:
        return bag, "skipped", "already converted", time.perf_counter() - start

    # A kept original is the pristine source when reconverting with --force.
    source = orig if orig.exists() else path
    try:
        with source.open("rb") as bagfile:
            if bagfile.read(len(ROS1_MAGIC)) != ROS1_MAGIC:
                msg = "not a ROS1 bag file"
                raise ValueError(msg)
    except Exception as exc:  # noqa: BLE001 - unreadable files cannot be converted
        return bag, "failed", str(exc), time.perf_counter() - start
    if not orig.exists() and not exclude_camera and ros1_compression(path) == "none":
        return bag, "skipped", "already uncompressed", time.perf_counter() - start

    try:
        if super_parallel:
            count = copy_uncompressed_parallel(source, tmp, max_workers, exclude_camera)
        else:
            count = copy_uncompressed(source, tmp, exclude_camera)
        if not orig.exists():
            _ = path.rename(orig)
        _ = tmp.replace(path)
    except Exception as exc:  # noqa: BLE001 - any failure means the bag is unconvertible
        tmp.unlink(missing_ok=True)
        return bag, "failed", str(exc), time.perf_counter() - start
    return bag, "converted", f"{count} messages", time.perf_counter() - start


def main() -> int:
    # Line-buffer stdout even when piped, so forked worker processes never
    # inherit (and re-flush) buffered output lines.
    if sys.stdout is not None and hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(
        prog="decompress_bag",
        description="Convert compressed ROS1 bags into uncompressed ROS1 bags.",
    )
    parser.add_argument(
        "bag", type=Path, help="path to a ROS1 bag file (or a folder with --folder)"
    )
    parser.add_argument(
        "--folder", action="store_true", help="recursively convert all *.bag files under the directory"
    )
    parser.add_argument(
        "--force", action="store_true", help="reconvert bags that were already converted"
    )
    parser.add_argument(
        "--super_parallel",
        "--super-parallel",
        action="store_true",
        dest="super_parallel",
        help=(
            "decompress each bag with several processes, buffering all of its messages"
            " in memory before writing (needs about as much free memory as the"
            " uncompressed bag); bags are processed one after another"
        ),
    )
    parser.add_argument(
        "--exclude-camera",
        action="store_true",
        help="drop sensor_msgs CameraInfo, Image, and CompressedImage topics from the output",
    )
    parser.add_argument("--quiet", action="store_true", help="do not show progress")
    parser.add_argument(
        "--max-workers", type=int, default=8, help="number of worker processes (default: 8)"
    )
    args = parser.parse_args()

    if not args.bag.exists():
        print(f"error: no such file or directory: {args.bag}", file=sys.stderr)
        return 1

    if args.folder:
        if not args.bag.is_dir():
            print(f"error: not a directory: {args.bag}", file=sys.stderr)
            return 1
        bags = find_bags(args.bag)
        if not bags:
            print(f"error: no bags found in {args.bag}", file=sys.stderr)
            return 1
    else:
        if not args.bag.is_file():
            print(f"error: not a ROS1 bag file: {args.bag}", file=sys.stderr)
            return 1
        bags = [args.bag]

    start = time.perf_counter()
    converted: list[str] = []
    skipped: list[str] = []
    failures: list[tuple[str, str]] = []

    def report(result: tuple[str, str, str | None, float], progress: tqdm) -> None:
        bag, status, detail, elapsed = result
        if status == "failed":
            failures.append((bag, detail or ""))
            progress.write(f"FAIL {bag}: {detail} ({elapsed:.1f}s)", file=sys.stderr)
        elif status == "skipped":
            skipped.append(bag)
            if not args.quiet:
                progress.write(f"SKIP {bag} ({detail})")
        else:
            converted.append(bag)
            if not args.quiet:
                progress.write(f"OK   {bag} ({detail}, {elapsed:.1f}s)")
        progress.update(1)

    if args.super_parallel:
        memory = available_memory()
        with tqdm(total=len(bags), unit="bag", disable=args.quiet) as progress:
            for bag in bags:
                if memory is not None and bag.stat().st_size * 3 > memory:
                    progress.write(
                        f"warning: {bag} may not fit in memory"
                        f" (~{bag.stat().st_size * 3 / 1e9:.1f} GB needed,"
                        f" {memory / 1e9:.1f} GB available)",
                        file=sys.stderr,
                    )
                if not args.quiet:
                    progress.write(f"checking {bag}")
                report(
                    decompress_bag(
                        str(bag), args.force, args.exclude_camera, True, args.max_workers
                    ),
                    progress,
                )
    else:
        with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
            futures = [
                executor.submit(decompress_bag, str(bag), args.force, args.exclude_camera)
                for bag in bags
            ]
            with tqdm(total=len(bags), unit="bag", disable=args.quiet) as progress:
                for future in as_completed(futures):
                    report(future.result(), progress)

    if failures:
        print(f"failed bags ({len(failures)}):", file=sys.stderr)
        for bag, error in sorted(failures):
            print(f"  {bag}: {error}", file=sys.stderr)
    if not args.quiet:
        print(
            f"processed {len(bags)} bags: {len(converted)} converted,"
            f" {len(skipped)} skipped, {len(failures)} failed"
            f" in {time.perf_counter() - start:.1f}s"
        )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
