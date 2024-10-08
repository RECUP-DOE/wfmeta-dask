Usage
=====

The possible inputs include:

- `-s` `--sched_file` : default `scheduler_transition.csv` \
The location of the file that contains all messages sent by the DASK scheduler.
- `-w` `--worker_file` : default `workers.csv` \
The location of the file that stores all worker creation messages.
- `-t` `--worker_trans_file` : default `worker_transition.csv` \
The location of the file that stores all worker state transition messages.
- `-x` `--worker_xfer_file` : default `worker_transfer.csv` \
The location of the file that stores all messages pertaining to file transfers between workers.
- `-o` `--output_file` : default `compiled_tasks.txt` \
Where to save the consolidated output.
- `-c` `--output_compressed` : default `compressed_out.pickle` \
Filename for the compressed consolidated output.

Usage:
```bash
python dask_capture.py -s scheduler_transition.csv -w workers.csv -t worker_transition.csv -x worker_transfer.csv -o compiled_tasks.txt
```