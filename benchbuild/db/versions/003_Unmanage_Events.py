"""
Remove 'benchbuild_events' from the managed part of the schema.

We do not delete this table during our upgrades,
because we do not want to wipe measurement data.

During downgrade we will make sure to create the table as needed.
"""
from sqlalchemy import (MetaData, SmallInteger, BigInteger, Numeric, Table,
                        Column, ForeignKey, Integer, String)
from benchbuild.utils.schema import Run, RunGroup, Project, Experiment

META = MetaData()

# yapf: disable
EVENTS = Table('benchbuild_events', META,
               Column('name', String, index=True),
               Column('start', Numeric, primary_key=True),
               Column('duration', Numeric),
               Column('id', Integer),
               Column('type', SmallInteger),
               Column('tid', BigInteger),
               Column('run_id', Integer,
                      ForeignKey('run.id', onupdate='CASCADE', ondelete='CASCADE'),
                      nullable=False, index=True, primary_key=True),
               extend_existing=True)
# yapf: enable


def upgrade(migrate_engine):
    pass


def downgrade(migrate_engine):
    META.bind = migrate_engine
    EVENTS.create(checkfirst=True)
