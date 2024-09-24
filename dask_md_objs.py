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

class TransferTypeEnum(Enum) :
    INCOMING = 'incoming_transfer'
    OUTGOING = 'outgoing_transfer'

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
              
class WXferEvent :
    start: datetime
    stop: datetime
    middle: datetime
    duration: float

    keys: Dict[str,int]

    total: int
    bandwith: float
    compressed: float

    requestor: str # ip addr; who
    fulfiller: str # called_from

    transfer_type: TransferTypeEnum

    time : datetime

    def __init__(self, data) :
        self.start = datetime.fromtimestamp(data['start'])
        self.stop = datetime.fromtimestamp(data['stop'])
        self.middle = datetime.fromtimestamp(data['middle'])
        self.duration = data['duration']

        self.keys = eval(data['keys'])

        self.total = data['total']
        self.bandwith = data['bandwidth']
        self.compressed = data['compressed']

        self.requestor = data['who']
        self.fulfiller = data['called_from']

        self.transfer_type = TransferTypeEnum(data['type'])

        self.time = datetime.fromtimestamp(data['time'])
    
    def __str__(self) -> str :
        out = "Worker Transfer Event (Type: {t})".format(t=self.transfer_type)
        out += "\n\tEvent time: {t}".format(t=self.time)
        out += "\n\tRequestor (Them): {r}\tFulfiller (Me): {f}".format(r=self.requestor, f=self.fulfiller)
        out += "\n\tStart: {s}\tMiddle: {m}\tEnd: {e}\t(Duration: {d})".format(
            s=self.start, m=self.middle, e=self.stop, d=self.duration
        )
        out += "\n\tTotal Transfer: {t}".format(t=self.total)
        out += "\n\tAffiliated Keys:"
        for key in self.keys :
            out += "\n\t\t{k}".format(k=key)
        return out
    
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
