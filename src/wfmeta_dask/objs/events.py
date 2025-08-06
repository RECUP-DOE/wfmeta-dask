from datetime import datetime
from typing import Dict, List, Tuple, Union

import pandas as pd

from ..helpers import generate_times
from .enums import TaskState, TransferTypeEnum, EventTypeEnum


class Event:
    # TODO: make useful
    #: The time this event was noted in a message.
    t_event: datetime
    #: The event type, for easy event filtering.
    e_type: EventTypeEnum


class TaskEvent(Event):
    #: The starting state of the task described in this message, as a :class:`~dask_md_objs.TaskState`.
    start: TaskState
    #: The ending state of the task described in this message, as a :class:`~dask_md_objs.TaskState`.
    finish: TaskState
    #: The key of the task that this TaskEvent is describing, as a string.
    key: str
    #: The IP address of the Event message source.
    ip: str


class SchedulerEvent(TaskEvent):
    """An Event that was sent by a Scheduler instance.

    Object representing an event that was sent by a DASK Scheduler instance.
    Contains information unique to messages sent by a DASK scheduler.
    """
    #: The `datetime` of the task start, if applicable.
    t_begins: Union[datetime, None]
    #: The `datetime` of the task end, if applicable.
    t_ends: Union[datetime, None]
    #: The stimulus ID of the scheduler event that caused this event message.
    stimulus_id: str

    e_type = EventTypeEnum.SCHEDULER

    # TODO : key, thread, worker, prefix, group
    def __init__(self, data: pd.DataFrame):
        """Initialize a new SchedulerEvent object.

        :param data: The pandas dataframe row that represents all data associated with a SchedulerEvent, including 'time', 'begins', 'ends', 'called_from', 'stimulus_id', and 'key'.
        :type data: pandas dataframe row
        """
        self.t_event, self.t_begins, self.t_ends = generate_times(
            {key: data[key]
             for key in ["time", "begins", "ends"]}
        )

        self.start = TaskState(data["start"])
        self.finish = TaskState(data["finish"])

        self.ip = str(data["called_from"])
        self.stimulus_id = str(data["stimulus_id"])

        self.key = str(data["key"])

    def __str__(self) -> str:
        return "Scheduler Event for task {e.key}\n".format(e=self) + \
              "\tEvent time: {e.t_event}\tBegin time: {e.t_begins}\tEnd time: {e.t_ends}\n".format(e=self) + \
              "\tStart: {e.start.value}\t\t\t\tFinish: {e.finish.value}\n".format(e=self) + \
              "\tSource: \n\t\t{source_info}\n".format(source_info="\n\t\t".join(self.ip.__str__().split("\n")))

    @staticmethod
    def to_df(events: List['SchedulerEvent']) -> pd.DataFrame:
        coll = []
        for e in events:
            coll.append([e.start, e.finish, e.key, e.t_begins, e.t_ends, e.ip, e.stimulus_id])

        df: pd.DataFrame = pd.DataFrame(
            coll,
            columns=["start", "finish", "key", "t_begins", "t_ends", "ip", "stimulus_id"],
            # dtype=[TaskState, TaskState, str, datetime, datetime, str, str]
            )

        return df


class WorkerEvent(TaskEvent):
    """Event that represents a change in Worker task state.

    """

    e_type = EventTypeEnum.WORKER

    def __init__(self, data):
        self.start = TaskState(data["start"])
        self.finish = TaskState(data["finish"])

        self.ip = data["called_from"]
        self.t_event = datetime.fromtimestamp(data["time"])
        self.key = data["key"]

    def __str__(self) -> str:
        return "Worker Event for task {e.key}\n".format(e=self) + \
            "\tEvent time: {e.t_event}\n".format(e=self) + \
            "\tStart: {e.start.value}\t\t\t\tFinish: {e.finish.value}\n".format(e=self) + \
            "\tSource: {e.ip}\n".format(e=self)

    @staticmethod
    def to_df(events: List['WorkerEvent']) -> pd.DataFrame:
        coll = []
        for e in events:
            coll.append([e.start, e.finish, e.key, e.ip])

        df: pd.DataFrame = pd.DataFrame(
            coll,
            columns=["start", "finish", "key", "ip"],
            # dtype=[TaskState, TaskState, str, str]
            )

        return df


class WXferEvent(Event):
    """An Event that represents a file transfer between Workers.

    An object that represents an event message that signifies a file has
    been transferred between two workers.
    """
    start: datetime
    stop: datetime
    middle: datetime
    duration: float

    keys: Dict[str, int]
    """Dictionary containing the `keys` information from the worker transfer message file.

    This dictionary is generating by `eval` ing the data directly."""

    total: int
    bandwidth: float
    #: nan for any Incoming transfer events.
    compressed: float

    #: IP address representing the `who` column of the worker transfer event - the other worker involved in this event.
    requestor: str  # ip addr; who
    #: IP address representing the `called_from` column of the worker transfer event - the worker that noted this event.
    fulfiller: str  # called_from

    #: The type of transfer this event describes, using a :class:`~dask_md_objs.TransferTypeEnum`. Can be either INCOMING or OUTGOING.
    transfer_type: TransferTypeEnum

    e_type = EventTypeEnum.WORKER_TRANSFER

    def __init__(self, data):
        #: This is an example docstring.
        self.start = datetime.fromtimestamp(data['start'])
        self.stop = datetime.fromtimestamp(data['stop'])
        self.middle = datetime.fromtimestamp(data['middle'])
        self.duration = data['duration']

        self.keys = eval(data['keys'])

        self.total = data['total']
        self.bandwidth = data['bandwidth']
        self.compressed = data['compressed']

        self.requestor = data['who']
        self.fulfiller = data['called_from']

        self.transfer_type = TransferTypeEnum(data['type'])

        self.t_event = datetime.fromtimestamp(data['time'])

    def __str__(self) -> str:
        out = "Worker Transfer Event (Type: {t})\n".format(t=self.transfer_type)
        out += "\tEvent time: {t}\n".format(t=self.t_event)
        out += "\tRequestor (Them): {r}\tFulfiller (Me): {f}\n".format(r=self.requestor, f=self.fulfiller)
        out += "\tStart: {s}\tMiddle: {m}\tEnd: {e}\t(Duration: {d})\n".format(
            s=self.start, m=self.middle, e=self.stop, d=self.duration
        )
        out += "\tTotal Transfer: {t}\n".format(t=self.total)
        out += "\tAffiliated Keys:"

        for key in self.keys:
            out += "\n\t\t{k}".format(k=key)

        out += "\n"
        return out

    @staticmethod
    def to_df(events: List[Tuple[str, 'WXferEvent']]) -> pd.DataFrame:
        coll = []
        for (cur_task, e) in events:
            coll.append([e.start, e.stop, e.middle, e.duration, cur_task, e.total, e.bandwidth, e.compressed, e.requestor, e.fulfiller, e.transfer_type, e.t_event])

        df: pd.DataFrame = pd.DataFrame(
            coll,
            columns=["start", "stop", "middle", "duration", "key", "total", "bandwidth", "compressed", "requestor", "fulfiller", "transfer_type", "t_event"],
            # dtype=[datetime, datetime, datetime, float, str, int, float, float, str, str, TransferTypeEnum, datetime]
            )
        return df

    def is_only_1_task(self) -> bool:
        """Returns whether this worker transfer event only relates to one task.

        :return: True if this wxferevent only relates to one task.
        :rtype: bool
        """
        return len(self.keys.keys()) == 1

    def n_tasks(self) -> int:
        """Returns the number of tasks this wxfer event is related to.

        :return: An integer representing the number of tasks this wxfer event is related to.
        :rtype: int
        """
        return len(list(self.keys.keys()))

    def get_key_name(self, i: int = 0) -> str:
        """Given an integer index, returns the name of the task at that position in its internal list of keys.

        :param i: The integer index of the desired task id, defaults to 0
        :type i: int, optional
        :return: The name of the task found
        :rtype: str
        """
        return list(self.keys.keys())[i]

    def return_key_names(self) -> List[str]:
        """Return all of the key names related to this wxfer event.

        :return: List of key names
        :rtype: List[str]
        """
        return list(self.keys.keys())

    def _check_most_equiv(self, other: 'WXferEvent') -> bool:
        """Checks most equivalencies between WXfer events to avoid repetition in identical_except_(fulfiller,requestor) and __eq__.

        Checks the equivalency of all internal attributes of Wxfer events except for requestor and fulfillor.
        In the case where the WXfer events are `TransferType.INCOMING`, `compressed` is ignored because `compressed` is not provided for incoming messages.

        :param other: Other WXfer event to compare against.
        :type other: WXferEvent
        :return: True if all attributes except requestor or fulfiller match.
        :rtype: bool
        """
        if not (self.start == other.start) or \
           not (self.stop == other.stop) or \
           not (self.middle == other.middle) or \
           not (self.duration == other.duration) or \
           not (self.keys == other.keys) or \
           not (self.total == other.total) or \
           not (self.bandwidth == other.bandwidth) or \
           not (self.transfer_type == other.transfer_type) or \
           not (self.t_event == other.t_event):
            return False

        elif self.transfer_type == TransferTypeEnum.OUTGOING and not (self.compressed == other.compressed):
            return False

        return True

    def identical_except_fulfiller(self, other: 'WXferEvent') -> bool:
        """Returns true if the given WXferEvent is identical except for the fulfiller field.

        Every attribute betwee this and the other wxfer event must match identically, and the fulfiller attributes _must_ differ.

        :param other: The WXferEvent to compare to
        :type other: WXferEvent
        :return: True if the events are identical except fulfiller
        :rtype: bool
        """
        if self._check_most_equiv(other):
            if (self.fulfiller != other.fulfiller) and (self.requestor == other.requestor):
                return True

        return False

    def identical_except_requestor(self, other: 'WXferEvent') -> bool:
        """Returns true if the given WXferEvent is identical except for the requestor field.

        Every attribute betwee this and the other wxfer event must match identically, and the requestor attributes _must_ differ.

        :param other: The WXferEvent to compare to
        :type other: WXferEvent
        :return: True if the events are identical except requestor
        :rtype: bool
        """
        if self._check_most_equiv(other):
            if (self.fulfiller == other.fulfiller) and (self.requestor != other.requestor):
                return True

        return False

    def __eq__(self, other) -> bool:
        if not isinstance(other, WXferEvent):
            if isinstance(other, SchedulerEvent):
                return False
            raise NotImplementedError("Cannot check if WXferEvent is equal to {}".format(type(other)))
        else:
            if self._check_most_equiv(other):
                if (self.fulfiller == other.fulfiller) and (self.requestor == other.requestor):
                    return True

            return False
