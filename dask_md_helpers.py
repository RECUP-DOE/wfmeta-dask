from datetime import datetime
from typing import Union

import numpy as np


def generate_times(sched_entry, debug=False) :
    def create_poss_nan_time(timestamp: str) -> Union[None, datetime] :
        if not np.isnan(timestamp) :
            return datetime.fromtimestamp(timestamp)
        else :
            return None

    t_event = datetime.fromtimestamp(sched_entry["time"])
    t_begins = create_poss_nan_time(sched_entry["begins"])
    t_ends = create_poss_nan_time(sched_entry["ends"])

    if debug:
        print("Time: {time}\tStart: {start}\tEnds: {end}".format(time=t_event, start=t_begins, end=t_ends))

    return(t_event, t_begins, t_ends)