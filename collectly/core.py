import datetime
from sqlalchemy import bindparam

from collectly import get_db_connection
from collectly.models import patients, payments


def filter_external_data(external_data, transform_fn):
    data_to_insert = []
    ids_to_delete = []
    for item in external_data:
        try:
            data_to_insert.append(transform_fn(item))
        except Exception:
            if 'externalId' in item:
                ids_to_delete.append({'external_id_to_delete': item['externalId']})
    return data_to_insert, ids_to_delete


def external_patient_to_internal(patient):
    return {
        'first_name': patient['firstName'],
        'last_name': patient['lastName'],
        'date_of_birth': datetime.datetime.strptime(patient['dateOfBirth'], '%Y-%m-%d').date(),
        'external_id': patient['externalId'],
        'updated': datetime.datetime.utcnow(),
        'deleted': False,
    }


def external_payment_to_internal(payment):
    return {
        'amount': payment['amount'],
        'patient_id': int(payment['patientId']),
        'external_id': payment['externalId'],
        'updated': datetime.datetime.utcnow(),
    }


def import_patients(patients_list):
    conn = get_db_connection()
    patients_to_insert, external_ids_to_delete = filter_external_data(patients_list, external_patient_to_internal)

    if patients_to_insert:
        external_ids = [p['external_id'] for p in patients_to_insert]

        existed_patients = conn.execute(patients.select()
                                        .where(patients.c.external_id.in_(external_ids))).fetchall()

        conn.execute(patients.insert(), patients_to_insert)

        existed_external_ids = [p['external_id'] for p in existed_patients]
        patients_to_update = [p for p in patients_to_insert if p['external_id'] in existed_external_ids]

        if not patients_to_update:
            return

        conn.execute(patients.update()
                     .where(patients.c.external_id == bindparam('external_id'))
                     .values({
                         'first_name': bindparam('first_name'),
                         'last_name': bindparam('last_name'),
                         'date_of_birth': bindparam('date_of_birth'),
                         'external_id': bindparam('external_id'),
                         'updated': bindparam('updated'),
                     }), patients_to_update)

    if external_ids_to_delete:
        conn.execute(patients.update()
                     .where(patients.c.external_id == bindparam('external_id_to_delete'))
                     .values({'deleted': True}), external_ids_to_delete)


def import_payments(payments_list):
    conn = get_db_connection()
    payments_to_insert, external_ids_to_delete = filter_external_data(payments_list, external_payment_to_internal)

    if payments_to_insert:
        external_ids = [p['external_id'] for p in payments_to_insert]

        existed_payments = conn.execute(payments.select()
                                        .where(payments.c.external_id.in_(external_ids))).fetchall()

        conn.execute(payments.insert(), payments_to_insert)

        existed_external_ids = [p['external_id'] for p in existed_payments]
        updated_payments = [p for p in payments_to_insert if p['external_id'] in existed_external_ids]

        if not updated_payments:
            return

        conn.execute(payments.update()
                     .where(payments.c.external_id == bindparam('external_id'))
                     .values({
                         'amount': bindparam('amount'),
                         'patient_id': bindparam('patient_id'),
                         'external_id': bindparam('external_id'),
                         'updated': bindparam('updated'),
                     }), updated_payments)

    if external_ids_to_delete:
        conn.execute(payments.update()
                     .where(payments.c.external_id == bindparam('external_id_to_delete'))
                     .values({'deleted': True}), external_ids_to_delete)
