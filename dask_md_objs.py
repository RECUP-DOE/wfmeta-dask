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

class Event:
    pass

class SchedulerEvent(Event) :
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
        return "Scheduler Event for task {e.task_id}\n".format(e=self) + \
              "\tEvent time: {e.t_event}\tBegin time: {e.t_begins}\tEnd time: {e.t_ends}\n".format(e=self) + \
              "\tStart: {e.start.value}\t\t\t\tFinish: {e.finish.value}\n".format(e=self) + \
              "\tSource: \n\t\t{source_info}\n".format(source_info = "\n\t\t".join(self.source.__str__().split("\n")))
              
class WXferEvent(Event) :
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
        out = "Worker Transfer Event (Type: {t})\n".format(t=self.transfer_type)
        out += "\tEvent time: {t}\n".format(t=self.time)
        out += "\tRequestor (Them): {r}\tFulfiller (Me): {f}\n".format(r=self.requestor, f=self.fulfiller)
        out += "\tStart: {s}\tMiddle: {m}\tEnd: {e}\t(Duration: {d})\n".format(
            s=self.start, m=self.middle, e=self.stop, d=self.duration
        )
        out += "\tTotal Transfer: {t}\n".format(t=self.total)
        out += "\tAffiliated Keys:"
        
        for key in self.keys :
            out += "\n\t\t{k}".format(k=key)

        out += "\n"
        return out
    
    def is_only_1_task(self) -> bool :
        if len(list(self.keys.keys())) == 1 :
            return True
        else :
            False

    def n_tasks(self) -> int :
        return len(list(self.keys.keys()))

    def get_key_name(self, i: int = 0) -> str :
        return list(self.keys.keys())[i]
    
class Task:
    name: str
    events: List[Event]

    t_start: datetime = None
    t_end: datetime = None

    workers: List[str]
    initiated: bool = False
    def __init__(self, first_event: Union[Event, None]) :
        self.events = []
        self.workers = []

        if first_event is not None :
            if isinstance(first_event, SchedulerEvent) :
                self.name = first_event.task_id
                self.initiated = True
                
                self.add_scheduler_event(first_event)
            elif isinstance(first_event, WXferEvent) :
                self.name = first_event.get_key_name()

                self.add_wxfer_event(first_event)
            else :
                raise ValueError("Unexpected Event Type in Task Construction.")

    def add_event(self, event_inp: Event) -> None:
        if isinstance(event_inp, SchedulerEvent) :
            self.add_scheduler_event(event_inp)
        elif isinstance(event_inp, WXferEvent) :
            if event_inp not in self.events :
                self.add_wxfer_event(event_inp)
        else :
            raise ValueError("Unexpected Event Type in Task.add_event()") 

    def add_wxfer_event(self, event_inp: WXferEvent) -> None :
        self.events.append(event_inp)

    def add_scheduler_event(self, event_inp: SchedulerEvent) -> None :
        self.events.append(event_inp)

        if event_inp.t_begins is not None :
            if self.t_start is None:
                self.t_start = event_inp.t_begins
            else :
                if event_inp.t_begins < self.t_start :
                    self.t_start = event_inp.t_begins

        if event_inp.t_ends is not None :
            if self.t_end is None:
                self.t_end = event_inp.t_ends
            else :
                if event_inp.t_ends < self.t_end :
                    self.t_end = event_inp.t_ends

    def __str__(self) -> str :
        event_strs = ""
        for e in self.events :
            event_strs += e.__str__()

        out = "Task object for task {e.name}:\n".format(e=self)
        out += "\tEvent objects:\n\t\t{event_info}".format(event_info = "\n\t\t".join(event_strs.split("\n")))
        out = out.strip()
        out += "\n\tStart time: {e.t_start}\tEnd time: {e.t_end}\n".format(e=self)

        return out

class TaskHandler :
    tasks: Dict[str, Task]

    def __init__(self) :
        self.tasks = {}
    
    def add_event(self, event: Event) -> None :
        if type(event) is SchedulerEvent :
            self._inner_add_event(event.task_id, event)
        elif type(event) is WXferEvent :
            if event.is_only_1_task() :
                e_id: str = event.get_key_name(0)
                self._inner_add_event(e_id, event)
            else :
                for i in range(0, event.n_tasks()) :
                    i_id: str = event.get_key_name(i)
                    self._inner_add_event(i_id, event)

    def _inner_add_event(self, id:str, event:Event) :
        if id not in self.tasks.keys() :
            temp_task = Task(event)
            self.tasks[id] = temp_task
        else :
            self.tasks[id].add_event(event)
