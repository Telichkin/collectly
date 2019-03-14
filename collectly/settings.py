import os
import tempfile

db_file = tempfile.NamedTemporaryFile()
root = os.path.dirname(os.path.dirname(__file__))


class Config(object):
    SECRET_KEY = 'REPLACE ME'


class ProdConfig(Config):
    ENV = 'prod'
    DATABASE_URI = 'sqlite:///' + os.path.join(root, 'database.db')


class DevConfig(Config):
    ENV = 'dev'
    DEBUG = True

    DATABASE_URI = 'sqlite:///' + os.path.join(root, 'database.db')


class TestConfig(Config):
    ENV = 'test'
    DEBUG = True

    DATABASE_URI = 'sqlite:///' + db_file.name
