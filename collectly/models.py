import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Base(object):
    __abstract__ = True
    id = sa.Column(sa.Integer, primary_key=True)
    created = sa.Column(sa.DateTime)
    updated = sa.Column(sa.DateTime)


Base = declarative_base(cls=Base)


class Patient(Base):
    __tablename__ = 'patients'

    first_name = sa.Column(sa.String, nullable=False)
    last_name = sa.Column(sa.String, nullable=False)
    middle_name = sa.Column(sa.String)
    date_of_birth = sa.Column(sa.Date)
    external_id = sa.Column(sa.String)


class Payment(Base):
    __tablename__ = 'payments'

    amount = sa.Column(sa.Float, nullable=False)
    patient_id = sa.Column(sa.Integer, sa.ForeignKey('patients.id'), nullable=False)
    external_id = sa.Column(sa.String)
