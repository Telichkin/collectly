from sqlalchemy import (
    MetaData, Table, Column, Integer,
    String, Float, Date, DateTime, ForeignKey
)


metadata = MetaData()


patients = Table(
    'patients', metadata,
    Column('id', Integer, primary_key=True),
    Column('created', DateTime),
    Column('updated', DateTime),

    Column('first_name', String, nullable=False),
    Column('last_name', String, nullable=False),
    Column('middle_name', String),
    Column('date_of_birth', Date),
    Column('external_id', String),
)


payments = Table(
    'payments', metadata,
    Column('id', Integer, primary_key=True),
    Column('created', DateTime),
    Column('updated', DateTime),

    Column('amount', Float, nullable=False),
    Column('patient_id', Integer, ForeignKey('patients.id'), nullable=False),
    Column('external_id', String),
)
