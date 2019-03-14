#! ../env/bin/python

from flask import Flask, g, current_app
from sqlalchemy import create_engine

from .api import api


def create_app(object_name):
    app = Flask(__name__)

    app.config.from_object(object_name)
    app.register_blueprint(api)
    app.before_request(create_db_connection)

    return app


def get_engine():
    if 'engine' not in g:
        g.engine = create_engine(
            current_app.config['DATABASE_URI'],
            echo=current_app.config.get('DATABASE_ECHO', False))

    return g.engine


def create_db_connection():
    if 'conn' not in g:
        g.conn = get_engine().connect()

    return g.conn
