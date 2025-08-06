import argparse as ap
from collections.abc import Callable
from typing import Dict, Optional

import pandas as pd
import pickle
import os

from wfmeta_dask.objs.enums import EventTypeEnum

from .objs import TaskHandler, WXferEvent, WorkerEvent, Event
from .objs import SchedulerEvent

from .helpers import create_verbose_function


def extract_metadata(filename: str, filecategory: str, debug: bool = False, th: Optional[TaskHandler] = None) -> TaskHandler:
    """Augments the provided TaskHandler with Event objects from the provided file, such that it can create new Events or augment existing ones with new Task information.
    Note that this function modifies the provided TaskHandler itself (aka it has side effects.)

    :param filename: the file to parse events from
    :type filename: str
    :param filecategory: the type of file being provided; must be one of ["SCHED", "WXFER", "WTRANS"]
    :type filecategory: str
    :param debug: whether to print the total number of tasks created and the first task, defaults to False
    :type debug: bool, optional
    :param th: the taskhandler to augment, defaults to None. If None, creates a new :class:`~dask_md_objs.TaskHandler`.
    :type th: :class:`~dask_md_objs.TaskHandler`, optional
    :raises ValueError: when an invalid filecategory is provided.
    :return: The provided taskhandler (or a new TaskHandler) augmented with the new events provided.
    :rtype: :class:`~dask_md_objs.TaskHandler`
    """
    # validate that provided filecategory is valid
    # this is only used internally so it's okay that it's just a magic value (for now).
    if filecategory not in ["SCHED", "WXFER", "WTRANS"]:
        # TODO : add examples of valid filecategories.
        raise ValueError("Invalid filecategory provided: {c}.".format(c=filecategory))

    dat: pd.DataFrame = pd.read_csv(filename)
    nrow, _ = dat.shape

    if th is None:
        th = TaskHandler()

    # store what type constructor to use based on filecategory
    eventtype: type = Event
    if filecategory == "SCHED":
        eventtype = SchedulerEvent
    elif filecategory == "WTRANS":
        eventtype = WorkerEvent
    elif filecategory == "WXFER":
        eventtype = WXferEvent

    for i in range(0, nrow):
        th.add_event(eventtype(dat.iloc(axis=0)[i]))

    if debug:
        print(len(th.tasks))
        print(th.tasks[list(th.tasks.keys())[0]])

    return th


if __name__ == "__main__":
    debug = False

    parser = ap.ArgumentParser(
                        prog='wfmeta-dask',
                        description='Extracts metadata objects from Dask-Mofka .csv files')
    parser.add_argument('-f', '--format',
                        choices=["txt", "pickle", "df_csv"],
                        help="Output format. TXT is prettyprint output, pickle are pickled python objects, and df_csv are dataframes serialized as csv.")
    parser.add_argument('-o', '--output',
                        help="Where to store the uncompressed output.")
    parser.add_argument('-c', '--compressed_output', default="compiled_tasks.pickle",
                        help="Where to store the compressed output.")
    parser.add_argument('--debug', action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose mode - print pretty statements through various points of the runtime.")
    parser.add_argument("directory")

    # parse
    args: ap.Namespace = parser.parse_args()

    verbose_print: Callable[[str], None] = create_verbose_function(args.verbose)

    directory = os.path.normpath(args.directory)
    output_file = args.output
    output_compressed = args.compressed_output
    debug = args.debug

    # make sure that they passed us a valid directory.
    if not os.path.exists(directory):
        raise ValueError("Provided directory path {path} does not exist.".format(path=directory))
    elif not os.path.isdir(directory):
        raise ValueError("Provided path {path} is not a directory. Please provide the path to the folder containing your Mofka-DASK output files.".format(path=directory))

    # make sure all the files we expect exist.
    sched_file: str = os.path.join(directory, "scheduler_transition.csv")
    if not os.path.isfile(sched_file):
        raise ValueError("There is no {file}.".format(file=sched_file))

    worker_xfer_file: str = os.path.join(directory, "worker_transfer.csv")
    if not os.path.isfile(worker_xfer_file):
        raise ValueError("There is no {file}.".format(file=worker_xfer_file))

    worker_trans_file: str = os.path.join(directory, "worker_transition.csv")
    if not os.path.isfile(worker_trans_file):
        raise ValueError("There is no {file}.".format(file=worker_trans_file))

    verbose_print("All files present and accounted for.")

    # off to the races
    th = TaskHandler()

    verbose_print("Extracting scheduler metadata.")
    extract_metadata(sched_file, "SCHED", debug, th)
    verbose_print("Extracting worker transfer metadata.")
    extract_metadata(worker_xfer_file, "WXFER", debug, th)
    verbose_print("Extracting worker metadata.")
    extract_metadata(worker_trans_file, "WTRANS", debug, th)

    verbose_print("Sorting compiled tasks.")
    th.sort_tasks_by_time()

    match args.format:
        case "txt":
            verbose_print("Saving txt file.")
            with open(output_file, "w") as f:
                keys: list[str] = list(th.tasks.keys())
                for i in range(0, len(keys)):
                    f.write(th.tasks[keys[i]].__str__())
        case "pickle":
            verbose_print("Saving pickle file of Python objects.")
            with open(output_compressed, 'wb') as f:
                pickle.dump(th, f, pickle.HIGHEST_PROTOCOL)
        case "df_csv":
            verbose_print("Saving pandas DataFrames serialized into csv files.")
            dfs: Dict[EventTypeEnum, pd.DataFrame] = th.to_df()
            for event_type, df in dfs.items():
                df.to_csv(args.output + "/" + event_type.name + "_df.csv")
        case _:
            raise ValueError("Somehow reached an unknown point when checking the format argument.")

    verbose_print("Done.")
