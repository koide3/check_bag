"""Command line interface for check_bag."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rosbags.highlevel import AnyReader
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


def show_info(reader: AnyReader, path: Path) -> None:
    """Print meta information of the bag."""
    size = bag_size(path)
    duration = reader.duration / 1e9
    print(f"path:     {path}")
    print(f"type:     {'ROS2' if reader.is2 else 'ROS1'}")
    print(f"size:     {human_size(size)} ({size} bytes)")
    print(f"duration: {duration:.3f} s")
    print(f"messages: {reader.message_count}")
    print(f"topics:   {len(reader.topics)}")
    width = max((len(name) for name in reader.topics), default=0)
    for name, topic in sorted(reader.topics.items()):
        print(f"  {name:<{width}}  {topic.msgtype}  ({topic.msgcount} msgs)")


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


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="check_bag",
        description="Check the validity of ROS1 and ROS2 bags.",
    )
    parser.add_argument("bag", type=Path, help="path to a ROS1 bag file or ROS2 bag directory")
    parser.add_argument("--de", action="store_true", help="deserialize messages (non-standard types are ignored)")
    parser.add_argument("--info", action="store_true", help="show meta information of the bag")
    parser.add_argument("--quiet", action="store_true", help="do not show progress")
    args = parser.parse_args()

    if not args.bag.exists():
        print(f"error: no such file or directory: {args.bag}", file=sys.stderr)
        return 1

    try:
        # Fallback typestore for bags without embedded type definitions;
        # definitions stored in the bag still take precedence.
        default_typestore = get_typestore(Stores.LATEST)
        with AnyReader([args.bag], default_typestore=default_typestore) as reader:
            if args.info:
                show_info(reader, args.bag)
            else:
                check_messages(reader, deserialize=args.de, quiet=args.quiet)
    except Exception as exc:  # noqa: BLE001 - any failure means the bag is invalid
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
