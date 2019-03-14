#! ../env/bin/python

from flask import Flask, g
from sqlalchemy import create_engine

from .api import api


def create_app(object_name):
    app = Flask(__name__)

    app.config.from_object(object_name)
    app.register_blueprint(api)

    with app.app_context():
        g.engine = create_engine(
            app.config['DATABASE_URI'],
            echo=app.config.get('DATABASE_ECHO', False))

    return app
