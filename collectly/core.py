import datetime
from sqlalchemy import bindparam, func, select, not_

from collectly.db import patients, payments


def filter_external_data(external_data, transform_fn):
    data = []

    for item in external_data:
        try:
            data.append(transform_fn(item))
        except:
            pass

    return data


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


def import_external(conn, data, table):
    imported_ids = [p['external_id'] for p in data]

    conn.execute(table
                 .update()
                 .where(not_(table.c.external_id.in_(imported_ids)))
                 .values({'deleted': True}))

    existed_data = conn.execute(table
                                .select()
                                .where(table.c.external_id.in_(imported_ids))).fetchall()

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


def import_patients(conn, patients_list):
    data = filter_external_data(patients_list, external_patient_to_internal)

    import_external(conn, data, patients)


def import_payments(conn, payments_list):
    data = filter_external_data(payments_list, external_payment_to_internal)

    # parent_id is internal id with autoincrement. Because of that, I can
    # find all existed patient_id's only by selecting max(parent_id)
    last_patient_id = conn.execute(func.max(patients.c.id)).scalar()
    data = [p for p in data if p['patient_id'] <= last_patient_id]

    import_external(conn, data, payments)


def get_patients(conn, min_amount=None, max_amount=None):
    query = patients.select().where(patients.c.deleted.is_(False))

    if min_amount or max_amount:
        total_amount = func.sum(payments.c.amount).label('total_amount')

        sub_query = (select([payments.c.patient_id])
                     .where(payments.c.deleted.is_(False))
                     .group_by(payments.c.patient_id))

        if min_amount:
            sub_query = sub_query.having(total_amount >= float(min_amount))

        if max_amount:
            sub_query = sub_query.having(total_amount <= float(max_amount))

        query = patients.join(sub_query, patients.c.id == sub_query.c.patient_id).select()

    return conn.execute(query).fetchall()


def get_payments(conn, external_id=None):
    query = payments.select().where(payments.c.deleted.is_(False))

    if external_id:
        sub_query = select([patients.c.id.label('p_id')]).where(patients.c.external_id == external_id)
        query = payments.join(sub_query, payments.c.patient_id == sub_query.c.p_id).select()

    return conn.execute(query).fetchall()
