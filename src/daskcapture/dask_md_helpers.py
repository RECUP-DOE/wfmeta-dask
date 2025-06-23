"""Miscellaneous helper methods to keep other mofka-dask capture methods code DRY.
"""

from datetime import datetime
from typing import Tuple, Union

import numpy as np

def generate_times(sched_entry, debug: bool = False) -> Tuple[datetime, Union[None, datetime], Union[None, datetime]]:
    """Generates datetime objects from a scheduler file entry where possible. There will always be at least one successful datetime object created from the "time" field of the scheduler entry.

    :param sched_entry: data from a dask scheduler transition csv file.
    :type sched_entry: pandas dataframe row
    :param debug: print generated dates to console, defaults to False
    :type debug: bool, optional
    :return: a 3-entry tuple containing either three datetime objects representing the "time", "begins", and "ends" fields (in that order), or a tuple containing a single datetime object representing the "time" field and two Nones.
    :rtype: Tuple[datetime, Optional[datetime], Optional[datetime]]
    """
    def create_poss_nan_time(timestamp: str) -> Union[None, datetime] :
        if not np.isnan(timestamp) :
            return datetime.fromtimestamp(float(timestamp))
        else :
            return None

    t_event = datetime.fromtimestamp(sched_entry["time"])
    t_begins = create_poss_nan_time(sched_entry["begins"])
    t_ends = create_poss_nan_time(sched_entry["ends"])

    if debug:
        print("Time: {time}\tStart: {start}\tEnds: {end}".format(time=t_event, start=t_begins, end=t_ends))

    return(t_event, t_begins, t_ends)