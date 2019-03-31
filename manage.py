""" Collectly sync

Usage:
    manage.py patients <path-to-json>
    manage.py payments <path-to-json>
"""
import os
import json

from docopt import docopt
from sqlalchemy import create_engine

from collectly.core import import_patients, import_payments


if __name__ == '__main__':
    arguments = docopt(__doc__)
    engine = create_engine(os.environ.get('DATABASE_URI'))
    conn = engine.connect()

    try:
        with open(arguments['<path-to-json>']) as data:
            if arguments['patients']:
                import_patients(conn, json.load(data))
            elif arguments['payments']:
                import_payments(conn, json.load(data))
    finally:
        conn.close()
