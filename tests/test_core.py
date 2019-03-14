import datetime

import pytest
from sqlalchemy import func

from collectly import create_app, create_db, drop_db, get_db_connection
from collectly.settings import TestConfig
from collectly.core import import_patients, import_payments
from collectly.models import patients, payments


@pytest.fixture(scope='session')
def app():

    return create_app(TestConfig)


@pytest.fixture(scope='function', autouse=True)
def db(app):
    with app.app_context():
        create_db()
        yield
        drop_db()


def test_add_new_patients_into_db(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
        ])

        conn = get_db_connection()
        res = conn.execute(patients.select().where(patients.c.external_id == 5)).fetchone()

        assert res['first_name'] == 'Rick'
        assert res['last_name'] == 'Deckard'
        assert res['external_id'] == '5'
        assert res['date_of_birth'] == datetime.date(year=2094, month=2, day=1)


def test_add_new_payments_into_db(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
        ])
        import_payments([
            {'amount': 4.46, 'patientId': '1', 'externalId': '501'}
        ])

        conn = get_db_connection()
        res = conn.execute(payments.select().where(payments.c.external_id == 501)).fetchone()

        assert res['amount'] == 4.46
        assert res['patient_id'] == 1
        assert res['external_id'] == '501'


def test_add_new_patients_with_corrupted_data(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Rick', 'lastName': 'Deckard', 'externalId': '5'},
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        ])

        conn = get_db_connection()
        count = conn.execute(func.count(patients)).scalar()

        assert count == 1


def test_add_new_payments_with_corrupted_data(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
        ])
        import_payments([
            {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
            {'patientId': '1', 'externalId': '502'},
        ])

        conn = get_db_connection()
        count = conn.execute(func.count(payments)).scalar()

        assert count == 1


def test_update_existed_patient(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        ])
        import_patients([
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-21', 'externalId': '4'},
        ])

        conn = get_db_connection()
        res = conn.execute(patients.select().where(patients.c.external_id == 4)).fetchone()

        assert res['date_of_birth'].day == 21


def test_update_existed_payment(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        ])
        import_payments([
            {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
        ])
        import_payments([
            {'amount': 5.12, 'patientId': '1', 'externalId': '501'},
        ])

        conn = get_db_connection()
        res = conn.execute(payments.select().where(payments.c.external_id == '501')).fetchone()

        assert res['amount'] == 5.12


def test_delete_corrupted_patients(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
            {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
        ])
        import_patients([
            {'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
            {'lastName': 'Unknown', 'dateOfBirth': '2099-12-18', 'externalId': '10'},
        ])

        conn = get_db_connection()
        deleted_patient = conn.execute(patients.select().where(patients.c.external_id == '5')).fetchone()
        total_count = conn.execute(func.count(patients)).scalar()

        assert deleted_patient['deleted'] is True
        assert total_count == 2


def test_delete_corrupted_payments(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        ])
        import_payments([
            {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
            {'amount': 5.12, 'patientId': '1', 'externalId': '512'},
        ])
        import_payments([
            {'amount': 4.46, 'externalId': '501'},
            {'amount': 3.21, 'externalId': '123'},
        ])

        conn = get_db_connection()
        deleted_payment = conn.execute(payments.select().where(payments.c.external_id == '501')).fetchone()
        total_count = conn.execute(func.count(payments)).scalar()

        assert deleted_payment['deleted'] is True
        assert total_count == 2
