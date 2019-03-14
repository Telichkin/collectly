#! ../env/bin/python


from flask import Flask, g, current_app


from collectly.api import api
from collectly.models import metadata
from collectly.db import close_db_connection


def create_app(object_name):
    app = Flask(__name__)

    app.config.from_object(object_name)
    app.register_blueprint(api)
    app.teardown_request(close_db_connection)

    return app
