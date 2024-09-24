## helper classes
from datetime import datetime
from enum import Enum
from typing import Dict, List, Union

from dask_md_helpers import generate_times

class TaskState(Enum) :
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

class SchedulerEvent :
    t_event : datetime
    t_begins : Union[datetime, None]
    t_ends : Union[datetime, None]

    start: TaskState
    finish: TaskState

    source: EventSource
    
    task_id: str
    # TODO : key, thread, worker, prefix, group
    def __init__(self, data) :
        self.t_event, self.t_begins, self.t_ends = generate_times(
            {key: data[key]
             for key in ["time","begins","ends"]}
        )

        self.start = TaskState(data["start"] )
        self.finish = TaskState(data["finish"])

        self.source = EventSource(data["called_from"], data["stimulus_id"])

        self.task_id = data["key"]

    def __str__(self) -> str :
        return "Event Object for task {e.task_id}\n".format(e=self) + \
              "\tEvent time: {e.t_event}\tBegin time: {e.t_begins}\tEnd time: {e.t_ends}\n".format(e=self) + \
              "\tStart: {e.start.value}\t\t\t\tFinish: {e.finish.value}\n".format(e=self) + \
              "\tSource: \n\t\t{source_info}\n".format(source_info = "\n\t\t".join(self.source.__str__().split("\n")))
              
class WXferEvent: 
    start: float
    stop: float
    middle: float
    duration: float
    
    def __init__(self, data) :
        return

class Task:
    name: str
    events: List[SchedulerEvent]

    t_start: datetime = None
    t_end: datetime = None

    workers: List[str]
    initiated: bool = False
    def __init__(self, first_event: Union[SchedulerEvent, None] = None) :
        self.events = []
        self.workers = []

        if first_event is not None :
            self.add_event(first_event)

    def add_event(self, first_event: SchedulerEvent) -> None :
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
    
    def add_event(self, event: SchedulerEvent) -> None :
        if event.task_id not in self.tasks.keys() :
            temp_task = Task(event)
            self.tasks[event.task_id] = temp_task
        
        else :
            self.tasks[event.task_id].add_event(event)
