import enum
import logging
import functools
from datetime import datetime

import attr
import rx
import sqlalchemy as sa
from plumbum import local

from benchbuild import signals
from benchbuild.utils import db
from benchbuild.utils.cmd import mkdir

LOG = logging.getLogger(__name__)


@enum.unique
class ActionStatus(enum.IntEnum):
    """Result type for action results."""
    UNSET = 0
    OK = 1
    CAN_CONTINUE = 2
    ERROR = 3


@attr.s
class Action:
    name = attr.ib()
    desc = attr.ib()

    action_obj = attr.ib(default=None)
    action_fn = attr.ib(default=None)

    status = attr.ib(default=ActionStatus.UNSET)

    def __call__(self):
        if self.action_fn:
            return self.action_fn()
        else:
            return None

    def __str__(self):
        return self.desc


def mkdir_(exp_or_prj):
    def make_builddir_fn():
        if not local.path(exp_or_prj.builddir).exists():
            mkdir("-p", exp_or_prj.builddir)
        return ActionStatus.OK

    return Action(
        "mkdir",
        "Create the build directory '{}'".format(exp_or_prj.builddir),
        action_obj=exp_or_prj,
        action_fn=make_builddir_fn)


def clean(exp_or_prj):
    def clean_builddir_fn():
        pass

    return Action(
        "clean",
        "Clean the build directory '{}'".format(exp_or_prj.builddir),
        action_obj=exp_or_prj,
        action_fn=clean_builddir_fn)


def compile_(project):
    return Action(
        "compile",
        "Compile the project",
        action_obj=project,
        action_fn=project.compile)


def run_(project):
    return Action(
        "run",
        "Execute the run action",
        action_obj=project,
        action_fn=project.run)


def echo(exp_or_prj, message):
    return Action(
        "echo",
        message,
        action_obj=exp_or_prj,
        action_fn=lambda: print(message))


def end_transaction(exp, session):
    try:
        if exp.end is None:
            exp.end = datetime.now()
        else:
            exp.end = max(exp.end, datetime.now())
        session.add(exp)
        session.commit()
    except sa.exc.InvalidRequestError as inv_req:
        LOG.error(inv_req)


def begin_transaction(exp):
    experiment, session = db.persist_experiment(exp)
    if experiment.begin is None:
        experiment.begin = datetime.now()
    else:
        experiment.begin = min(experiment.begin, datetime.now())
    session.add(experiment)
    try:
        session.commit()
    except sa.orm.exc.StaleDataError:
        LOG.error("Transaction isolation level caused a StaleDataError")

    # React to external signals
    signals.handlers.register(end_transaction, experiment, session)

    return experiment, session


def observable_experiment(exp, actions):
    def observable(observer, scheduler=None):
        echo_start = echo(exp, "Start experiment.")
        echo_stop = echo(exp, "Completed experiment.")

        experiment, session = begin_transaction(exp)
        observer.on_next(echo_start)
        for action in actions:
            observer.on_next(action)
        observer.on_next(echo_stop)
        end_transaction(experiment, session)

    return observable
