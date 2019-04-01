import datetime
import sqlite3

from sqlalchemy import (
    event, engine, MetaData, Table, Column, Integer,
    Boolean, String, Float, Date, DateTime, ForeignKey, Index
)


metadata = MetaData()


def table(name, *columns):
    return Table(
        name, metadata,
        Column('id', Integer, primary_key=True),
        Column('created', DateTime, default=datetime.datetime.utcnow),
        Column('updated', DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow),
        Column('deleted', Boolean, default=False),
        Column('external_id', String),
        *columns)


patients = table(
    'patients',

    Column('first_name', String, nullable=False),
    Column('last_name', String, nullable=False),
    Column('middle_name', String),
    Column('date_of_birth', Date),
    Index('idx_patients_external_id', 'external_id'))


payments = table(
    'payments',

    Column('amount', Float, nullable=False),
    Column('patient_id', Integer, ForeignKey('patients.id'), nullable=False),
    Index('idx_payments_external_id', 'external_id'))


@event.listens_for(engine.Engine, 'connect')
def set_sqlite_pragma(dbapi_connection, _):
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute('PRAGMA foreign_keys=ON;')
        cursor.close()
