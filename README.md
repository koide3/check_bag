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

# Skip camera messages (sensor_msgs CameraInfo, Image, and CompressedImage).
uvx check_bag bag_filename --exclude-camera

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

## decompress_bag

Convert compressed ROS1 bags into uncompressed ones (uncompressed bags read
much faster, e.g. for repeated playback or checking).

```bash
# Convert one bag: the original is kept as bag_filename.orig.bag and the
# uncompressed bag takes its place. Skipped if a *.orig.bag already exists.
uvx --from check-bag decompress_bag bag_filename

# Recursively convert every *.bag under a directory, using worker processes
# (default: 8). Already converted and already uncompressed bags are skipped.
uvx --from check-bag decompress_bag directory --folder --max-workers 8

# Reconvert bags that were already converted, using the kept *.orig.bag as
# the source.
uvx --from check-bag decompress_bag directory --folder --force

# Drop camera topics (sensor_msgs CameraInfo, Image, and CompressedImage)
# from the output bag. The kept *.orig.bag still holds every message.
uvx --from check-bag decompress_bag bag_filename --exclude-camera

# Decompress each bag with several processes instead of one process per bag.
uvx --from check-bag decompress_bag directory --folder --super_parallel --max-workers 16
```

By default each bag is converted by a single process and several bags are
converted concurrently, which is the fastest option for a folder of bags.
With `--super_parallel` the bags are instead converted one after another, each
using `--max-workers` processes to decompress its chunks in parallel; all of
the bag's messages are buffered in memory and written in one pass once its
decompression is done. This is much faster for a single large bag, but it
needs roughly as much free memory as the uncompressed bag (a warning is
printed when a bag looks too large to fit). Combining it with
`--exclude-camera` keeps the buffer small.

Conversion is atomic: the new bag is written to a temporary file first, so a
failure never leaves a partially written bag or renames the original. Bags
that fail are listed at the end and the exit code is 1.
