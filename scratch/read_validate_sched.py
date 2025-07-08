# depreciated
import pandas as pd
import numpy as np
from enum import Enum
from datetime import datetime

from typing import Dict, List, Union # TODO: update to python 3.10 and use | instead

## helper classes
class EventState(Enum) :
    # enums for the various event states
    # TODO : put into a logical order, perhaps can check if QUEUED > RELEASED later.
    RELEASED = 'released'
    WAITING = 'waiting'
    QUEUED = 'queued'
    PROCESSING = 'processing'
    MEMORY = 'memory'
    FORGOTTEN = 'forgotten'

class EventSource :
    addr = None
    stim_id = None

    def __init__(self, addr, stim_id) :
        self.addr = addr
        self.stim_id = stim_id

    def __str__(self) -> str :
        return "Called from: {e.addr}\nStimulus ID: {e.stim_id}".format(e=self)

class Event :
    t_event : datetime
    t_begins : Union[datetime, None]
    t_ends : Union[datetime, None]

    start: EventState
    finish: EventState

    source: EventSource
    
    task_id: str
    # TODO : key, thread, worker, prefix, group
    def __init__(self, data) :
        self.t_event, self.t_begins, self.t_ends = generate_times(
            {key: data[key]
             for key in ["time","begins","ends"]}
        )

        self.start = EventState(data["start"] )
        self.finish = EventState(data["finish"])

        self.source = EventSource(data["called_from"], data["stimulus_id"])

        self.task_id = data["key"]

    def __str__(self) -> str :
        return "Event Object for task {e.task_id}\n".format(e=self) + \
              "\tEvent time: {e.t_event}\tBegin time: {e.t_begins}\tEnd time: {e.t_ends}\n".format(e=self) + \
              "\tStart: {e.start.value}\t\t\t\tFinish: {e.finish.value}\n".format(e=self) + \
              "\tSource: \n\t\t{source_info}\n".format(source_info = "\n\t\t".join(self.source.__str__().split("\n")))
              
class Task:
    name: str
    events: List[Event]

    t_start: datetime = None
    t_end: datetime = None

    workers: List[str]
    initiated: bool = False
    def __init__(self, first_event: Union[Event, None] = None) :
        self.events = []
        self.workers = []

        if first_event is not None :
            self.add_event(first_event)

    def add_event(self, first_event: Event) -> None :
        self.name = first_event.task_id
        self.events.append(first_event)
        self.initiated = True

        if first_event.t_begins is not None :
            if self.t_start is None:
                self.t_start = first_event.t_begins
            else :
                if first_event.t_begins < self.t_start :
                    self.t_start = first_event.t_begins

        if first_event.t_ends is not None :
            if self.t_end is None:
                self.t_end = first_event.t_ends
            else :
                if first_event.t_ends < self.t_end :
                    self.t_end = first_event.t_ends

    def __str__(self) -> str :
        event_strs = ""
        for e in self.events :
            event_strs += e.__str__()

        return "Task object for task {e.name}:\n".format(e=self) + \
                "\tEvent objects:\n\t\t{event_info}\n".format(event_info = "\n\t\t".join(event_strs.split("\n"))) + \
                "Start time: {e.t_start}\tEnd time: {e.t_end}".format(e=self)

class TaskHandler :
    tasks: Dict[str, Task]

    def __init__(self) :
        self.tasks = {}
    
    def add_event(self, event: Event) -> None :
        if event.task_id not in self.tasks.keys() :
            temp_task = Task(event)
            self.tasks[event.task_id] = temp_task
        
        else :
            self.tasks[event.task_id].add_event(event)


## helper fxns
def pretty_print_line(entry) :
    for x in sched_x.columns :
        print("{column_name} : {column_value}".format(column_name = x, column_value = sched_x[x][entry]))

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

def begins_lt_ends(begins, ends, debug=False) :
    begin_nans = np.isnan(begins)
    end_nans = np.isnan(ends)

    if not np.array_equal(begin_nans, end_nans) :
        raise ValueError("Begins and Ends are not NaN at the same time.")
    
    n_nan = np.count_nonzero(begin_nans)
    begin_vals = begins[~begin_nans]
    end_vals = ends[~end_nans]

    if not len(begins) == (n_nan + len(begin_vals)) :
        raise ValueError("Somehow, combination of number of nans and number of found non-nan values is not equal to the number of original values.")
    
    diff = np.subtract(begins, ends)
    begin_after_end = diff >= 0
    n_begin_after_end = np.count_nonzero(begin_after_end)

    if n_begin_after_end > 0 :
        raise ValueError("Assumption that Begin is always before End did not hold true.")
    
    if debug :
        print("Total elements: {n}\tNaN: {nan}\tBegin Vals: {bvals}\tEnd Vals: {evals}\nNumber of times Begin > End: {ngt}".format(n=len(begins), nan=n_nan, bvals=len(begin_vals), evals=len(end_vals), ngt=n_begin_after_end))

def begins_eq_gt_time(begins, time, debug=False) :
    # this tells us that time != begins, and that begin is often before time.
    #   TODO: why?
    begin_nans = np.isnan(begins)

    n_begin_nan = np.count_nonzero(begin_nans)
    n_non_nan = len(time) - n_begin_nan

    non_na_begin = begins[~begin_nans]
    non_na_time = time[~begin_nans]

    diff = np.subtract(non_na_begin, non_na_time)
    n_non_equal = np.count_nonzero(diff)

    begin_after_time = diff >= 0
    n_after_time = np.count_nonzero(begin_after_time)

    if debug:
        print("Total elements: {n}\tTotal Non-NaN Begin: {n2}\tTotal Non-Equivalent: {n3}\tTotal Begin >= Time: {n4}".format(n=len(time), n2=n_non_nan, n3=n_non_equal, n4=n_after_time))
    
## main functionality
scheduler_file_loc = "./scheduler_transition.csv" # TODO : adjust to be an input param

sched_x = pd.read_csv(scheduler_file_loc)
nrow,ncol = sched_x.shape
begins_lt_ends(sched_x["begins"], sched_x["ends"], debug=True)
begins_eq_gt_time(sched_x["begins"], sched_x["time"], debug=True)
t = Event(sched_x.iloc(axis=0)[0])

th = TaskHandler()
for i in range(0, nrow) :
    th.add_event(Event(sched_x.iloc(axis=0)[i]))

print(len(th.tasks))
print(th.tasks[list(th.tasks.keys())[0]])

with open("compiled_tasks.txt", "w") as f:
    keys = list(th.tasks.keys())
    for i in range(0, len(keys)) :
        f.write(th.tasks[keys[i]].__str__())