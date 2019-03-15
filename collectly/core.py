import datetime
from sqlalchemy import bindparam, func, select

from collectly.db import get_db_connection
from collectly.models import patients, payments


def filter_external_data(external_data, transform_fn):
    data = []
    ids_to_delete = []

    for item in external_data:
        try:
            data.append(transform_fn(item))
        except Exception:
            if 'externalId' in item:
                ids_to_delete.append({'external_id_to_delete': item['externalId']})
    return data, ids_to_delete


def external_patient_to_internal(patient):
    return {
        'first_name': patient['firstName'],
        'last_name': patient['lastName'],
        'middle_name': patient.get('middleName'),
        'date_of_birth': datetime.datetime.strptime(patient['dateOfBirth'], '%Y-%m-%d').date(),
        'external_id': patient['externalId'],
        'deleted': False,
    }


def external_payment_to_internal(payment):
    return {
        'amount': payment['amount'],
        'patient_id': int(payment['patientId']),
        'external_id': payment['externalId'],
        'deleted': False,
    }


def has_diff(item_for_update, item_from_db):
    item_from_db = dict(item_from_db)
    item_from_db.pop('id', None)
    item_from_db.pop('created', None)
    item_from_db.pop('updated', None)

    return item_for_update != item_from_db


def import_external(data, external_ids_to_delete, table):
    conn = get_db_connection()

    if data:
        external_ids = [p['external_id'] for p in data]
        existed_data = conn.execute(table
                                    .select()
                                    .where(table.c.external_id.in_(external_ids))).fetchall()

        existed_data_by_external_id = {p['external_id']: p for p in existed_data}

        data_to_insert = [p for p in data if p['external_id'] not in existed_data_by_external_id]

        if data_to_insert:
            conn.execute(table.insert(), data_to_insert)

        data_to_update = [{**p, 'external_id_to_update': p['external_id']} for p in data
                          if p['external_id'] in existed_data_by_external_id
                          and has_diff(p, existed_data_by_external_id[p['external_id']])]

        if data_to_update:
            conn.execute(table
                         .update()
                         .where(table.c.external_id == bindparam('external_id_to_update')), data_to_update)

    if external_ids_to_delete:
        conn.execute(table.update()
                     .where(table.c.external_id == bindparam('external_id_to_delete'))
                     .values({'deleted': True}), external_ids_to_delete)


def import_patients(patients_list):
    data, external_ids_to_delete = filter_external_data(patients_list, external_patient_to_internal)

    import_external(
        data,
        external_ids_to_delete,
        table=patients)


def import_payments(payments_list):
    conn = get_db_connection()
    data_to_insert, external_ids_to_delete = filter_external_data(payments_list, external_payment_to_internal)

    # parent_id is internal id with autoincrement. Because of that, I can
    # find all existed patient_id's only by selecting max(parent_id)
    last_patient_id = conn.execute(func.max(patients.c.id)).scalar()

    import_external(
        [p for p in data_to_insert if p['patient_id'] <= last_patient_id],
        external_ids_to_delete,
        table=payments)


def get_patients(min_amount=None, max_amount=None):
    conn = get_db_connection()
    query = patients.select().where(patients.c.deleted.is_(False))

    if min_amount or max_amount:
        total_amount = func.sum(payments.c.amount).label('total_amount')

        sub_query = (select([payments.c.patient_id])
                     .where(payments.c.deleted.is_(False))
                     .group_by(payments.c.patient_id))

        if min_amount:
            sub_query = sub_query.having(total_amount >= int(min_amount))

        if max_amount:
            sub_query = sub_query.having(total_amount <= int(max_amount))

        query = patients.join(sub_query, patients.c.id == sub_query.c.patient_id).select()

    return conn.execute(query).fetchall()


def get_payments(external_id=None):
    conn = get_db_connection()
    query = payments.select().where(payments.c.deleted.is_(False))

    if external_id:
        sub_query = select([patients.c.id.label('p_id')]).where(patients.c.external_id == external_id)
        query = payments.join(sub_query, payments.c.patient_id == sub_query.c.p_id).select()

    return conn.execute(query).fetchall()
