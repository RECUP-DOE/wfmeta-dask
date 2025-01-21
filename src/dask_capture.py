import argparse as ap

import pandas as pd
import pickle

import os

from dask_md_objs import SchedulerEvent, TaskHandler, WXferEvent, WorkerEvent

## TODO :: REMOVE ipykernel from UV

def extract_metadata(filename: str, filecategory: str, debug: bool = False, th: TaskHandler = None) -> TaskHandler :
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
    if filecategory not in ["SCHED", "WXFER", "WTRANS"] :
        # TODO : add examples of valid filecategories.
        raise ValueError("Invalid filecategory provided: {c}.".format(c=filecategory))

    dat = pd.read_csv(filename)
    nrow, _ = dat.shape

    if th is None :
        th = TaskHandler()
    
    # store what type constructor to use based on filecategory
    eventtype : type = None
    if filecategory == "SCHED" :
        eventtype = SchedulerEvent
    elif filecategory == "WTRANS" :
        eventtype = WorkerEvent
    elif filecategory == "WXFER" :
        eventtype = WXferEvent

    for i in range(0, nrow) :
        th.add_event(eventtype(dat.iloc(axis=0)[i]))
    
    if debug:
        print(len(th.tasks))
        print(th.tasks[list(th.tasks.keys())[0]])

    return th

if __name__ == "__main__":
    debug = False

    parser = ap.ArgumentParser(
                        prog='DaskParser',
                        description='Extracts metadata objects from Dask-Mofka .csv files')

    parser.add_argument('-o', '--output', default="compiled_tasks.txt", 
                        help="Where to store the uncompressed output.")
    parser.add_argument('-c', '--compressed_output', default="compiled_tasks.pickle",
                         help="Where to store the compressed output.")
    parser.add_argument('--debug', action="store_true")
    parser.add_argument("directory")

    # parse
    args = parser.parse_args()

    directory = os.path.normpath(args.directory)
    output_file = args.output
    output_compressed = args.compressed_output
    debug = args.debug

    # make sure that they passed us a valid directory.
    if not os.path.exists(directory) :
        raise ValueError("Provided directory path {path} does not exist.".format(path=directory))
    elif not os.path.isdir(directory) :
        raise ValueError("Provided path {path} is not a directory. Please provide the path to the folder containing your Mofka-DASK output files.".format(path=directory))

    # make sure all the files we expect exist.
    sched_file = os.path.join(directory, "scheduler_transition.csv")
    if not os.path.isfile(sched_file) :
        raise ValueError("There is no {file}.".format(file = sched_file))
    
    worker_xfer_file = os.path.join(directory, "worker_transfer.csv")
    if not os.path.isfile(worker_xfer_file) :
        raise ValueError("There is no {file}.".format(file = worker_xfer_file))
    
    worker_trans_file = os.path.join(directory, "worker_transition.csv")
    if not os.path.isfile(worker_trans_file) :
        raise ValueError("There is no {file}.".format(file = worker_trans_file))

    print("All files present and accounted for.")

    # off to the races
    th = TaskHandler()

    # confirmed identical to previous implementation.
    print("Extracting scheduler metadata.")
    extract_metadata(sched_file, "SCHED", debug, th)
    print("Extracting worker transfer metadata.")
    extract_metadata(worker_xfer_file, "WXFER", debug, th)
    print("Extracting worker metadata.")
    extract_metadata(worker_trans_file, "WTRANS", debug, th)

    print("Sorting compiled tasks.")
    th.sort_tasks_by_time()

    print("Saving raw file.")
    if output_file is not None :
       with open(output_file, "w") as f:
            keys = list(th.tasks.keys())
            for i in range(0, len(keys)) :
                f.write(th.tasks[keys[i]].__str__())

    print("Saving compressed file.")
    with open(output_compressed, 'wb') as f:
        pickle.dump(th, f, pickle.HIGHEST_PROTOCOL)

    print("Done.")