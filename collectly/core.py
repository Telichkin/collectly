import datetime

from collectly import get_db_connection
from collectly.models import patients, payments


def external_patient_to_internal(patient, updated_at):
    return {
        'first_name': patient['firstName'],
        'last_name': patient['lastName'],
        'date_of_birth': datetime.datetime.strptime(patient['dateOfBirth'], '%Y-%m-%d').date(),
        'external_id': patient['externalId'],
        'updated': updated_at,
    }


def external_payment_to_internal(payment, updated_at):
    return {
        'amount': payment['amount'],
        'patient_id': int(payment['patientId']),
        'external_id': payment['externalId'],
        'updated': updated_at,
    }


def import_patients(patients_list):
    conn = get_db_connection()
    updated_at = datetime.datetime.utcnow()

    conn.execute(
        patients.insert(),
        [external_patient_to_internal(p, updated_at) for p in patients_list])


def import_payments(payments_list):
    conn = get_db_connection()
    updated_at = datetime.datetime.utcnow()

    conn.execute(
        payments.insert(),
        [external_payment_to_internal(p, updated_at) for p in payments_list])
