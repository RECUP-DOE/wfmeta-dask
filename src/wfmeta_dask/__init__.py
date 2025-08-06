from typing import Optional

import pandas as pd

from .objs import TaskHandler, WXferEvent, WorkerEvent, Event
from .objs import SchedulerEvent


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