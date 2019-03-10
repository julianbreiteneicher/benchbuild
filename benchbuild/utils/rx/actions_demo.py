#%%
import logging
import multiprocessing as mp
import threading

import rx
from rx import operators as op
from plumbum import ProcessExecutionError

from benchbuild.experiments import raw
from benchbuild.projects.benchbuild.bzip2 import Bzip2
from benchbuild.projects.benchbuild.linpack import Linpack
from benchbuild.projects.test.test import TestProject, TestProjectRuntimeFail
from benchbuild.utils.rx.actions import (Action, ActionStatus,
                                         observable_experiment, clean,
                                         compile_, echo, mkdir_, run_)

RAW = raw.RawRuntime()
PRJ = TestProject(RAW)
PRJ_FAIL = TestProjectRuntimeFail(RAW)
LINPACK = Linpack(RAW)
BZIP = Bzip2(RAW)

LOG = logging.getLogger(__name__)


def log_error(error):
    LOG.error("\n==== ERROR ====")
    LOG.error("Action failed because: %s", str(error))
    LOG.error("==== ERROR ====\n")


def log_action(actn: Action, index):
    prj_or_exp = actn.action_obj
    if prj_or_exp:
        LOG.info("#{} * {}: {}".format(index, prj_or_exp.name, str(actn)))
    return actn


def run_action(actn: Action):
    try:
        res = actn()
    except Exception as a:
        res = ActionStatus.ERROR
        log_error(a)

    return res


def to_status(result):
    if not result:
        return ActionStatus.OK
    return ActionStatus.OK


def action_sequence(prj):
    return [
        clean(prj),
        mkdir_(prj),
        echo(prj, "Selected {0} with version {1}".format(
            prj.name, prj.version)),
        compile_(prj),
        run_(prj),
        clean(prj)
    ]


actions_good = action_sequence(PRJ)
actions_linpack = action_sequence(LINPACK)
actions_bzip2 = action_sequence(BZIP)
actions_bad = action_sequence(PRJ_FAIL)

sched = rx.concurrency.CurrentThreadScheduler()

#sched = rx.concurrency.ThreadPoolScheduler()


def create_experiment_pipe(experiment, actions):
    return rx.create(observable_experiment(experiment, actions)).pipe(
        op.subscribe_on(sched), \
        op.map_indexed(log_action), \
        op.map(run_action), \
        op.map(to_status), \
        op.filter(lambda res: res != ActionStatus.OK))


all_exps = rx.merge(
    create_experiment_pipe(RAW, actions_good), \
    create_experiment_pipe(RAW, actions_linpack), \
    create_experiment_pipe(RAW, actions_bzip2))

all_exps.subscribe(
    on_next=lambda a: print("Failed, with result: ", a),
    on_error=lambda e: print(e))
input("Press any key to continue...")
