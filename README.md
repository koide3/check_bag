# check_bag

Check the validity of ROS1 and ROS2 bags. Internally it uses
[rosbags](https://gitlab.com/ternaris/rosbags) to read bag data, so it works
without a ROS installation.

## Usage

```bash
# Check if the bag is valid: reads all messages without deserialization.
# Shows progress unless --quiet is given. Exits with 1 on any error, 0 otherwise.
uvx check_bag bag_filename

# Read all messages with deserialization. Non-standard (unresolvable) types are ignored.
uvx check_bag bag_filename --de

# Show meta information: size, duration, number of messages, topics (name and type).
uvx check_bag bag_filename --info

# Quick check: only verify that the bag opens and that its first and last
# messages are valid (deserialized too when --de is given).
uvx check_bag bag_filename --quick

# Check all ROS1 and ROS2 bags found under a directory (recursively).
# --max-workers sets the number of worker processes (default: 8).
uvx check_bag directory --folder --max-workers 8
```

With `--folder`, all ROS1 bags share one global pool of `--max-workers`
processes that scans their chunks (heavy bags queued first: compressed before
uncompressed, larger before smaller), so every worker stays busy until the
whole queue drains. ROS2 bags each run as a single-process task in the same
pool and are submitted first so they overlap the chunk work. Each result line
reports the per-bag processing time, and the summary reports the total time.
Any bags that failed are listed again at the end of the run, so failures are
easy to find in a long log.
Setting `--max-workers` to the number of CPU cores gives the fastest checks
for compressed bags.

Single ROS1 bags are likewise checked by scanning their chunks in parallel
with `--max-workers` processes, which greatly speeds up compressed (bz2/lz4)
bags.


`bag_filename` can be a ROS1 `.bag` file or a ROS2 bag directory.

## Development

```bash
uv sync
uv run check_bag path/to/bag
```
