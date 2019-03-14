#!/usr/bin/env python

import os
import json

from flask_script import Manager, Server
from flask_script.commands import ShowUrls, Clean, Command

from collectly import create_app
from collectly.db import get_engine, create_db
from collectly.core import import_patients, import_payments


# default to dev config because no one should use this in
# production anyway
env = os.environ.get('APPNAME_ENV', 'dev')
app = create_app('collectly.settings.%sConfig' % env.capitalize())


def import_patients_from_json(path):
    with open(path) as patients_in_file:
        import_patients(json.load(patients_in_file))


def import_payments_from_json(path):
    with open(path) as payments_in_file:
        import_payments(json.load(payments_in_file))


manager = Manager(app)
manager.add_command('server', Server(port=9090))
manager.add_command('show-urls', ShowUrls())
manager.add_command('clean', Clean())
manager.add_command('createdb', Command(create_db))
manager.add_command('import-patients', Command(import_patients_from_json))
manager.add_command('import-payments', Command(import_payments_from_json))


@manager.shell
def make_shell_context():
    """ Creates a python REPL with several default imports
        in the context of the app
    """
    return dict(app=app, engine=get_engine())


if __name__ == "__main__":
    manager.run()
