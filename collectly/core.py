import datetime
from sqlalchemy import bindparam

from collectly import get_db_connection
from collectly.models import patients, payments


def filter_external_data(external_data, transform_fn):
    res = []
    for item in external_data:
        try:
            res.append(transform_fn(item))
        except Exception:
            continue
    return res


def external_patient_to_internal(patient):
    return {
        'first_name': patient['firstName'],
        'last_name': patient['lastName'],
        'date_of_birth': datetime.datetime.strptime(patient['dateOfBirth'], '%Y-%m-%d').date(),
        'external_id': patient['externalId'],
        'updated': datetime.datetime.utcnow(),
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
    internal_patients = filter_external_data(patients_list, external_patient_to_internal)
    external_ids = [p['external_id'] for p in internal_patients]

    existed_patients = conn.execute(patients.select()
                                    .where(patients.c.external_id.in_(external_ids))).fetchall()

    conn.execute(patients.insert(), internal_patients)

    existed_external_ids = [p['external_id'] for p in existed_patients]
    updated_patients = [p for p in internal_patients if p['external_id'] in existed_external_ids]

    if not updated_patients:
        return

    conn.execute(patients.update()
                 .where(patients.c.external_id == bindparam('external_id'))
                 .values({
                     'first_name': bindparam('first_name'),
                     'last_name': bindparam('last_name'),
                     'date_of_birth': bindparam('date_of_birth'),
                     'external_id': bindparam('external_id'),
                     'updated': bindparam('updated'),
                 }), updated_patients)


def import_payments(payments_list):
    conn = get_db_connection()
    internal_payments = filter_external_data(payments_list, external_payment_to_internal)
    external_ids = [p['external_id'] for p in internal_payments]

    existed_payments = conn.execute(payments.select()
                                    .where(payments.c.external_id.in_(external_ids))).fetchall()

    conn.execute(payments.insert(), internal_payments)

    existed_external_ids = [p['external_id'] for p in existed_payments]
    updated_payments = [p for p in internal_payments if p['external_id'] in existed_external_ids]

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
