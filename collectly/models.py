import datetime

from sqlalchemy import (
    MetaData, Table, Column, Integer, Boolean,
    String, Float, Date, DateTime, ForeignKey, Index
)


metadata = MetaData()


def table(name, *columns):
    return Table(
        name, metadata,
        Column('id', Integer, primary_key=True),
        Column('created', DateTime, default=datetime.datetime.utcnow),
        Column('updated', DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow),
        Column('deleted', Boolean, default=False),
        *columns,
    )


patients = table(
    'patients',

    Column('first_name', String, nullable=False),
    Column('last_name', String, nullable=False),
    Column('middle_name', String),
    Column('date_of_birth', Date),
    Column('external_id', String),
    Index('idx_patients_external_id', 'external_id'),
)


payments = table(
    'payments',

    Column('amount', Float, nullable=False),
    Column('patient_id', Integer, ForeignKey('patients.id'), nullable=False),
    Column('external_id', String),
    Index('idx_payments_external_id', 'external_id'),
)