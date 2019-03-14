#! ../env/bin/python

import sqlite3
from sqlalchemy import create_engine, event, engine
from flask import Flask, g, current_app


from collectly.api import api
from collectly.models import metadata


def create_app(object_name):
    app = Flask(__name__)

    app.config.from_object(object_name)
    app.register_blueprint(api)

    return app


def get_engine():
    if 'engine' not in g:
        g.engine = create_engine(
            current_app.config['DATABASE_URI'],
            echo=current_app.config.get('DATABASE_ECHO', False))

    return g.engine


def get_db_connection():
    if 'conn' not in g:
        g.conn = get_engine().connect()

    return g.conn


def create_db():
    metadata.create_all(get_engine())


def drop_db():
    metadata.drop_all(get_engine())


@event.listens_for(engine.Engine, 'connect')
def set_sqlite_pragma(dbapi_connection, _):
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute('PRAGMA foreign_keys=ON;')
        cursor.close()
