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
```

`bag_filename` can be a ROS1 `.bag` file or a ROS2 bag directory.

## Development

```bash
uv sync
uv run check_bag path/to/bag
```
