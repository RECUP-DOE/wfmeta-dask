from enum import Enum

class TaskState(Enum) :
    """Describes the possible states of a :class:`~dask_md_obj.Task`
    """
    # TODO : put into a logical order, perhaps can check if QUEUED > RELEASED later.
    RELEASED = 'released'
    WAITING = 'waiting'
    QUEUED = 'queued'
    PROCESSING = 'processing'
    MEMORY = 'memory'
    FORGOTTEN = 'forgotten'

    # Worker-Specific States
    READY = 'ready'
    EXECUTING = 'executing'
    FETCH = 'fetch'
    FLIGHT = 'flight'
    CANCELLED = 'cancelled' # TODO: look into this one

class TransferTypeEnum(Enum) :
    """Describes whether a :class:`~dask_md_obj.WXferEvent` represents an incoming or outgoing file transfer.
    """
    INCOMING = 'incoming_transfer'
    OUTGOING = 'outgoing_transfer'

class EventTypeEnum(Enum) :
    SCHEDULER = 'scheduler'
    WORKER = 'worker'
    WORKER_TRANSFER = 'wxfer'