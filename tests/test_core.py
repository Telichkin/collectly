import datetime

import pytest

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
        conn = get_db_connection()
        import_patients([
            {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
        ])

        res = conn.execute(patients.select().where(patients.c.external_id == 5)).fetchone()

        assert res['first_name'] == 'Rick'
        assert res['last_name'] == 'Deckard'
        assert res['external_id'] == '5'
        assert res['date_of_birth'] == datetime.date(year=2094, month=2, day=1)


def test_add_new_payments_into_db(app):
    with app.app_context():
        conn = get_db_connection()
        import_patients([
            {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
        ])
        import_payments([
            {'amount': 4.46, 'patientId': '1', 'externalId': '501'}
        ])

        res = conn.execute(payments.select().where(payments.c.external_id == 501)).fetchone()

        assert res['amount'] == 4.46
        assert res['patient_id'] == 1
        assert res['external_id'] == '501'
