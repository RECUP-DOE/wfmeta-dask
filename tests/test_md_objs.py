from datetime import timedelta, datetime
import random
from typing import Dict
import json
import pytest

from dask_md_objs import TransferTypeEnum, WXferEvent

def generate_random_taskname() -> str :
    output: str = "('array-8838909ee9756e34565341881e2e6f0c', {n})".format(n=str(random.randint(0,500)))
    return output

def generate_random_keys(n: int = 1) -> Dict[str, int] :
    output: Dict[str, int] = {}
    for i in range(0, n) :
        newname = generate_random_taskname()
        while newname in output.keys() :
            newname = generate_random_taskname()
        output[newname] = 336
    
    return output

def generate_random_ip(not_this_ip: str = None) -> str :
    output = "tcp://{a:02n}.{b:03n}.{c:n}.{d:03n}:{e:5f}".format(a = random.randint(1,99), 
                    b = random.randint(1,999), c = random.randint(0,9), d = random.randint(1,999),
                    e = random.randint(1,99999))
    
    if not_this_ip :
        if output == not_this_ip :
            output = generate_random_ip(output)
    
    return output

def generate_dummy_wxfer(transfer_type: TransferTypeEnum = None, n_keys = 1) -> WXferEvent:
    curtime: datetime = datetime.now()

    start: datetime = curtime - timedelta(hours=1, minutes=0)
    stop: datetime = curtime
    middle: datetime = curtime - timedelta(hours=0, minutes=30)
    duration: float = (stop - start).total_seconds()

    keys: Dict[str, int] = generate_random_keys(n_keys)
    keys_str: str = json.dumps(keys)

    total: int = random.randint(0,100)
    bandwidth: float = random.random() * 100
    compressed: float = random.random() * total

    requestor: str = generate_random_ip()
    fulfiller: str = generate_random_ip(requestor)

    if not transfer_type :
        transfer_type = [TransferTypeEnum.INCOMING, TransferTypeEnum.OUTGOING][random.randint(0,1)]
    
    time : datetime = curtime

    data = {'start': int(round(start.timestamp())), 
            'stop' : int(round(stop.timestamp())),
            'middle': int(round(middle.timestamp())),
            'duration': duration,
            'keys': keys_str,
            'total': total,
            'bandwidth': bandwidth,
            'compressed': compressed,
            'who': requestor,
            'called_from': fulfiller,
            'type': transfer_type.value,
            'time': int(round(time.timestamp()))}
    
    return WXferEvent(data)

def test_creatingDummyXfer() :
    dummy = generate_dummy_wxfer(TransferTypeEnum.INCOMING)
    assert dummy.transfer_type == TransferTypeEnum.INCOMING

    dummy = generate_dummy_wxfer(TransferTypeEnum.OUTGOING)
    assert dummy.transfer_type == TransferTypeEnum.OUTGOING

    dummy = generate_dummy_wxfer(n_keys=2)
    assert dummy.n_tasks() == 2

def test_equivalencyXfer() :
    dummy_inc = generate_dummy_wxfer(TransferTypeEnum.INCOMING)
    dummy_inc_2 = generate_dummy_wxfer(TransferTypeEnum.INCOMING)
    dummy_out = generate_dummy_wxfer(TransferTypeEnum.OUTGOING)
    dummy_out_2 = generate_dummy_wxfer(TransferTypeEnum.OUTGOING)

    assert dummy_inc.__eq__(dummy_inc)
    assert (dummy_inc == dummy_inc)
    assert not (dummy_inc == dummy_out)
    assert not (dummy_inc == dummy_inc_2)

    assert (dummy_out == dummy_out)
    assert not (dummy_out == dummy_out_2)


def test_creatingTaskHandler() :
    assert True