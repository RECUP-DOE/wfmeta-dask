# Should eventually be moved into the combination repository
import pytest
from wfmeta_dask import extract_metadata
from pathlib import Path
from shutil import copy

from wfmeta_dask.objs.tasks import TaskHandler

def test_simple(tmpdir) :
    # Copy example files
    i_sc = Path("./tests/test_data/scheduler_transition.csv")
    o_sc = Path(tmpdir / "scheduler_transition.csv")
    copy(str(i_sc), str(o_sc))

    i_wo = Path("./tests/test_data/worker_transition.csv")
    o_wo = Path(tmpdir / "worker_transition.csv")
    copy(str(i_wo), str(o_wo))

    i_wx = Path("./tests/test_data/worker_transfer.csv")
    o_wx = Path(tmpdir / "worker_transfer.csv")
    copy(str(i_wx), str(o_wx))

    # run using dummy files
    # TODO: should be a better function to do this, the runner shouldn't have to worry about
    #   creating a taskhandler or doing each file at a time.
    th = TaskHandler()
    
    options_sc = [o_sc, "SCHED", True, th]
    options_wo = [o_wo, "WTRANS", True, th]
    options_wx = [o_wx, "WXFER", True, th]

    extract_metadata(*options_sc)
    extract_metadata(*options_wo)
    extract_metadata(*options_wx)

    pass