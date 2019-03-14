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


def import_external(data, table, transform_fn):
    conn = get_db_connection()
    data_to_insert, external_ids_to_delete = filter_external_data(data, transform_fn)

    if data_to_insert:
        external_ids = [p['external_id'] for p in data_to_insert]
        existed_data = conn.execute(table.select()
                                    .where(table.c.external_id.in_(external_ids))).fetchall()

        conn.execute(table.insert(), data_to_insert)

        existed_external_ids = [p['external_id'] for p in existed_data]
        data_to_update = [{**p, 'external_id_to_update': p['external_id']} for p in data_to_insert
                          if p['external_id'] in existed_external_ids]

        if not data_to_update:
            return

        conn.execute(table.update()
                     .where(table.c.external_id == bindparam('external_id_to_update')), data_to_update)

    if external_ids_to_delete:
        conn.execute(table.update()
                     .where(table.c.external_id == bindparam('external_id_to_delete'))
                     .values({'deleted': True}), external_ids_to_delete)


def import_patients(patients_list):
    import_external(
        data=patients_list,
        table=patients,
        transform_fn=external_patient_to_internal,
    )


def import_payments(payments_list):
    import_external(
        data=payments_list,
        table=payments,
        transform_fn=external_payment_to_internal,
    )
