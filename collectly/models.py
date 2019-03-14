import datetime

from sqlalchemy import (
    MetaData, Table, Column, Integer,
    String, Float, Date, DateTime, ForeignKey
)


metadata = MetaData()


def table(name, *columns):
    return Table(
        name, metadata,
        Column('id', Integer, primary_key=True),
        Column('created', DateTime, default=datetime.datetime.utcnow),
        Column('updated', DateTime),
        *columns,
    )


patients = table(
    'patients',

    Column('first_name', String, nullable=False),
    Column('last_name', String, nullable=False),
    Column('middle_name', String),
    Column('date_of_birth', Date),
    Column('external_id', String),
)


payments = table(
    'payments',

    Column('amount', Float, nullable=False),
    Column('patient_id', Integer, ForeignKey('patients.id'), nullable=False),
    Column('external_id', String),
)
