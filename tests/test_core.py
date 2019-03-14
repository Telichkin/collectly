import datetime

import pytest
from sqlalchemy import func

from collectly import create_app, create_db, drop_db, get_db_connection
from collectly.settings import TestConfig
from collectly.core import import_patients, import_payments, get_patients
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
        payment = conn.execute(payments.select().where(payments.c.external_id == '501')).fetchone()

        assert payment['amount'] == 5.12


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


def test_return_deleted_payment(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        ])
        import_patients([
            {'firstName': 'Pris', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        ])
        import_patients([
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        ])

        conn = get_db_connection()
        patient = conn.execute(patients.select().where(patients.c.external_id == '4')).fetchone()

        assert patient['deleted'] == False
        assert patient['last_name'] == 'Stratton'


def test_corrupted_foreign_key_should_not_prevent_importing(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        ])
        import_payments([
            {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
            {'amount': 5.12, 'patientId': '2', 'externalId': '512'},
        ])

        conn = get_db_connection()
        count = conn.execute(func.count(payments)).scalar()

        assert count == 1


def test_get_patients(app):
    with app.app_context():
        import_patients([
            {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
            {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
            {'firstName': 'Roy', 'lastName': 'Batti', 'dateOfBirth': '2093-06-12', 'externalId': '8'},
            {'firstName': 'Eldon', 'lastName': 'Tyrell', 'dateOfBirth': '2056-04-01', 'externalId': '15'},
        ])
        import_payments([
            {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
            {'amount': 5.66, 'patientId': '1', 'externalId': '502'},
            {'amount': 7.1, 'patientId': '1', 'externalId': '503'},
            {'amount': 23.32, 'patientId': '3', 'externalId': '601'},
            {'amount': 2.29, 'patientId': '3', 'externalId': '602'},
            {'amount': 9.29, 'patientId': '4', 'externalId': '701'},
        ])

        all_patients = get_patients()
        patients_with_amount_more_than_25 = get_patients(min_amount=25)
        patients_with_amount_between_10_and_20 = get_patients(min_amount=9, max_amount=20)

        assert len(all_patients) == 4
        assert len(patients_with_amount_more_than_25) == 1
        assert patients_with_amount_more_than_25[0]['first_name'] == 'Roy'
        assert len(patients_with_amount_between_10_and_20) == 2
        assert patients_with_amount_between_10_and_20[0]['first_name'] == 'Rick'
