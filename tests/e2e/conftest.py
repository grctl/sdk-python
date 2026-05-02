import multiprocessing
import sys

# On Linux, multiprocessing defaults to "fork", which deadlocks when the parent
# process has threads (e.g. NATS client I/O threads). macOS already defaults to
# "spawn" since Python 3.8, so this only matters on Linux CI.
if sys.platform == "linux":
    multiprocessing.set_start_method("spawn", force=True)
