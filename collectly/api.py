import os
import json
import datetime
from collections import namedtuple

from sqlalchemy import create_engine

from collectly.core import get_patients, import_patients, get_payments, import_payments


Request = namedtuple('Request', ('method', 'path', 'query_str', 'body'))


def environ_to_request(environ):
    return Request(
        method=environ['REQUEST_METHOD'],
        path=environ['PATH_INFO'],
        query_str=environ['QUERY_STRING'],
        body=environ['wsgi.input'])


def create_endpoint(handle_request, engine):
    def dump_datetime(obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()

    def endpoint(environ, start_fn):
        request = environ_to_request(environ)
        connection = engine.connect()

        try:
            code, response = handle_request(request, connection)
            response_str = json.dumps(response, default=dump_datetime).encode('utf-8')
        finally:
            connection.close()

        start_fn(code, [
            ('Content-Type', 'application/json; charset=utf-8'),
            ('Content-Length', str(len(response_str))),
        ])
        return [response_str]

    return endpoint


def query_str_to_dict(query_str):
    query_dict = {}
    for pair in query_str.split('&'):
        if pair:
            key, value = pair.split('=')
            query_dict[key] = value
    return query_dict


def body_to_object(body):
    body_str = body.read().decode('utf-8')
    return json.loads(body_str)


def handler(request, conn):
    if request.path == '/patients' and request.method == 'GET':
        query = query_str_to_dict(request.query_str)
        patients = get_patients(
            conn,
            min_amount=query.get('payment_min'),
            max_amount=query.get('payment_max'))

        return '200 OK', [dict(patient) for patient in patients]

    if request.path == '/patients' and request.method == 'POST':
        import_patients(conn, body_to_object(request.body))

        return '200 OK', {'status': 'OK'}

    if request.path == '/payments' and request.method == 'GET':
        query = query_str_to_dict(request.query_str)
        payments = get_payments(
            conn,
            external_id=query.get('external_id'))

        return '200 OK', [dict(payment) for payment in payments]

    if request.path == '/payments' and request.method == 'POST':
        import_payments(conn, body_to_object(request.body))

        return '200 OK', {'status': 'OK'}

    return '404 Not Found', {'status': 'error', 'code': '404'}


def create_app():
    engine = create_engine(os.environ.get('DATABASE_URI'))

    return create_endpoint(handler, engine)


application = create_app()
