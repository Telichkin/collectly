from flask import Blueprint, jsonify, request

from collectly.core import get_patients, get_payments, import_patients, import_payments

api = Blueprint('api', __name__)


def patients_get():
    min_amount = request.args.get('payment_min')
    max_amount = request.args.get('payment_max')

    # patients' json can be formatted in any other way
    return jsonify([dict(p) for p in get_patients(min_amount=min_amount, max_amount=max_amount)])


def patients_post():
    import_patients(request.get_json())
    return jsonify({'status': 'OK'})


def payments_get():
    external_id = request.args.get('external_id')

    # payments' json can be formatted in an other way
    return jsonify([dict(p) for p in get_payments(external_id=external_id)])


def payments_post():
    import_payments(request.get_json())
    return jsonify({'status': 'OK'})


@api.route('/patients', methods=['POST', 'GET'])
def patients():
    method = patients_post if request.method == 'POST' else patients_get
    return method()


@api.route('/payments', methods=['POST', 'GET'])
def payments():
    method = payments_post if request.method == 'POST' else payments_get
    return method()
