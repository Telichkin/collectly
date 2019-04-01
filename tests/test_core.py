import tempfile
import datetime

import pytest
from sqlalchemy import create_engine, func

from collectly.core import import_patients, import_payments, get_patients, get_payments
from collectly.db import metadata, patients, payments


@pytest.fixture(scope='session')
def engine():
    db_uri = 'sqlite:///' + tempfile.NamedTemporaryFile().name
    return create_engine(db_uri, echo=False)


@pytest.fixture(scope='function', autouse=True)
def db(engine):
    metadata.create_all(engine)
    yield
    metadata.drop_all(engine)


@pytest.fixture()
def conn(engine):
    connection = engine.connect()
    yield connection
    connection.close()


def test_add_new_patients_into_db(conn):
    import_patients(conn, [
        {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
    ])

    res = conn.execute(patients.select().where(patients.c.external_id == 5)).fetchone()

    assert res['first_name'] == 'Rick'
    assert res['last_name'] == 'Deckard'
    assert res['external_id'] == '5'
    assert res['date_of_birth'] == datetime.date(year=2094, month=2, day=1)


def test_add_new_payments_into_db(conn):
    import_patients(conn, [
        {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
    ])
    import_payments(conn, [
        {'amount': 4.46, 'patientId': '1', 'externalId': '501'}
    ])

    res = conn.execute(payments.select().where(payments.c.external_id == 501)).fetchone()

    assert res['amount'] == 4.46
    assert res['patient_id'] == 1
    assert res['external_id'] == '501'


def test_add_new_patients_with_corrupted_data(conn):
    import_patients(conn, [
        {'firstName': 'Rick', 'lastName': 'Deckard', 'externalId': '5'},
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])

    count = conn.execute(func.count(patients)).scalar()

    assert count == 1


def test_add_new_payments_with_corrupted_data(conn):
    import_patients(conn, [
        {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
    ])
    import_payments(conn, [
        {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
        {'patientId': '1', 'externalId': '502'},
    ])

    count = conn.execute(func.count(payments)).scalar()

    assert count == 1


def test_update_existed_patient(conn):
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-21', 'externalId': '4'},
    ])

    res = conn.execute(patients.select().where(patients.c.external_id == 4)).fetchone()
    count = conn.execute(func.count(patients)).scalar()

    assert res['date_of_birth'].day == 21
    assert count == 1


def test_update_existed_payment(conn):
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])
    import_payments(conn, [
        {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
    ])
    import_payments(conn, [
        {'amount': 5.12, 'patientId': '1', 'externalId': '501'},
    ])

    payment = conn.execute(payments.select().where(payments.c.external_id == '501')).fetchone()

    assert payment['amount'] == 5.12


def test_delete_corrupted_patients(conn):
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
    ])
    import_patients(conn, [
        {'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
        {'lastName': 'Unknown', 'dateOfBirth': '2099-12-18', 'externalId': '10'},
    ])

    deleted_patient = conn.execute(patients.select().where(patients.c.external_id == '5')).fetchone()
    total_count = conn.execute(func.count(patients)).scalar()

    assert deleted_patient['deleted'] is True
    assert total_count == 2


def test_delete_corrupted_payments(conn):
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])
    import_payments(conn, [
        {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
        {'amount': 5.12, 'patientId': '1', 'externalId': '512'},
    ])
    import_payments(conn, [
        {'amount': 4.46, 'externalId': '501'},
        {'amount': 3.21, 'externalId': '123'},
    ])

    deleted_payment = conn.execute(payments.select().where(payments.c.external_id == '501')).fetchone()
    total_count = conn.execute(func.count(payments)).scalar()

    assert deleted_payment['deleted'] is True
    assert total_count == 2


def test_delete_patients_which_exist_in_db_but_not_exist_in_data(conn):
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
    ])
    import_patients(conn, [
        {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
    ])

    pris = conn.execute(patients.select().where(patients.c.external_id == '4')).fetchone()

    assert pris['deleted'] is True


def test_return_deleted_payment(conn):
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])
    import_patients(conn, [
        {'firstName': 'Pris', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])

    patient = conn.execute(patients.select().where(patients.c.external_id == '4')).fetchone()

    assert patient['deleted'] is False
    assert patient['last_name'] == 'Stratton'


def test_corrupted_foreign_key_should_not_prevent_importing(conn):
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])
    import_payments(conn, [
        {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
        {'amount': 5.12, 'patientId': '2', 'externalId': '512'},
    ])

    count = conn.execute(func.count(payments)).scalar()

    assert count == 1


def test_change_updated_only_after_real_update(conn):
    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])
    initial_patient = conn.execute(patients.select()).fetchone()

    import_patients(conn, [
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])
    not_updated_patient = conn.execute(patients.select()).fetchone()

    import_patients(conn, [
        {'firstName': 'Pray', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
    ])
    updated_patient = conn.execute(patients.select()).fetchone()

    assert initial_patient['updated'] == not_updated_patient['updated']
    assert initial_patient['updated'] != updated_patient['updated']


def test_get_patients(conn):
    import_patients(conn, [
        {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        {'firstName': 'Roy', 'lastName': 'Batti', 'dateOfBirth': '2093-06-12', 'externalId': '8'},
        {'firstName': 'Eldon', 'lastName': 'Tyrell', 'dateOfBirth': '2056-04-01', 'externalId': '15'},
    ])
    import_payments(conn, [
        {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
        {'amount': 5.66, 'patientId': '1', 'externalId': '502'},
        {'amount': 7.1, 'patientId': '1', 'externalId': '503'},
        {'amount': 23.32, 'patientId': '3', 'externalId': '601'},
        {'amount': 2.29, 'patientId': '3', 'externalId': '602'},
        {'amount': 9.29, 'patientId': '4', 'externalId': '701'},
    ])

    all_patients = get_patients(conn)
    patients_with_amount_more_than_25 = get_patients(conn, min_amount='25')
    patients_with_amount_between_10_and_20 = get_patients(conn, min_amount='9', max_amount='20')

    assert len(all_patients) == 4
    assert len(patients_with_amount_more_than_25) == 1
    assert patients_with_amount_more_than_25[0]['first_name'] == 'Roy'
    assert len(patients_with_amount_between_10_and_20) == 2
    assert patients_with_amount_between_10_and_20[0]['first_name'] == 'Rick'


def test_get_payments(conn):
    import_patients(conn, [
        {'firstName': 'Rick', 'lastName': 'Deckard', 'dateOfBirth': '2094-02-01', 'externalId': '5'},
        {'firstName': 'Pris', 'lastName': 'Stratton', 'dateOfBirth': '2093-12-20', 'externalId': '4'},
        {'firstName': 'Roy', 'lastName': 'Batti', 'dateOfBirth': '2093-06-12', 'externalId': '8'},
        {'firstName': 'Eldon', 'lastName': 'Tyrell', 'dateOfBirth': '2056-04-01', 'externalId': '15'},
    ])
    import_payments(conn, [
        {'amount': 4.46, 'patientId': '1', 'externalId': '501'},
        {'amount': 5.66, 'patientId': '1', 'externalId': '502'},
        {'amount': 7.1, 'patientId': '1', 'externalId': '503'},
        {'amount': 23.32, 'patientId': '3', 'externalId': '601'},
        {'amount': 2.29, 'patientId': '3', 'externalId': '602'},
        {'amount': 9.29, 'patientId': '4', 'externalId': '701'},
    ])

    all_payments = get_payments(conn)
    ricks_payments = get_payments(conn, external_id='5')
    pris_payments = get_payments(conn, external_id='4')

    assert len(all_payments) == 6
    assert len(ricks_payments) == 3
    assert len(pris_payments) == 0
