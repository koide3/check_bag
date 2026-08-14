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

# Check all ROS1 and ROS2 bags found under a directory (recursively).
# --max-workers sets the number of worker processes (default: 8).
uvx check_bag directory --folder --max-workers 8
```

With `--folder`, bags are ordered heavy-first (compressed ROS1 bags before
uncompressed ones, larger before smaller) and dispatched against a budget of
`--max-workers` worker processes: each ROS1 bag scans its chunks with
`--max-workers-per-bag` processes, while each ROS2 bag uses a single process,
so light bags fill the cores freed by heavy ones. Each result line reports
the per-bag processing time, and the summary reports the total time.

Single ROS1 bags are likewise checked by scanning their chunks in parallel
with `--max-workers` processes, which greatly speeds up compressed (bz2/lz4)
bags.


`bag_filename` can be a ROS1 `.bag` file or a ROS2 bag directory.

## Development

```bash
uv sync
uv run check_bag path/to/bag
```
