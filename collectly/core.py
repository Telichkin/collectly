import datetime
from sqlalchemy import bindparam, func, select

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


def import_external(data_to_insert, external_ids_to_delete, table):
    conn = get_db_connection()

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
    data_to_insert, external_ids_to_delete = filter_external_data(patients_list, external_patient_to_internal)

    import_external(
        data_to_insert,
        external_ids_to_delete,
        table=patients,
    )


def import_payments(payments_list):
    data_to_insert, external_ids_to_delete = filter_external_data(payments_list, external_payment_to_internal)

    conn = get_db_connection()
    last_patient_id = conn.execute(func.max(patients.c.id)).scalar()

    import_external(
        [p for p in data_to_insert if p['patient_id'] <= last_patient_id],
        external_ids_to_delete,
        table=payments,
    )


def get_patients(min_amount=None, max_amount=None):
    conn = get_db_connection()
    query = patients.select().where(patients.c.deleted.is_(False))

    if min_amount or max_amount:
        total_amount = func.sum(payments.c.amount).label('total_amount')

        sub_query = (select([payments.c.patient_id])
                     .where(payments.c.deleted.is_(False))
                     .group_by(payments.c.patient_id))

        if min_amount:
            sub_query = sub_query.having(total_amount >= min_amount)

        if max_amount:
            sub_query = sub_query.having(total_amount <= max_amount)

        query = query.where(patients.c.id.in_(sub_query))

    return conn.execute(query).fetchall()


def get_payments(external_id=None):
    conn = get_db_connection()
    query = payments.select().where(payments.c.deleted.is_(False))

    if external_id:
        sub_query = select([patients.c.id]).where(patients.c.external_id == external_id)
        query = query.where(payments.c.patient_id == sub_query)

    return conn.execute(query).fetchall()
