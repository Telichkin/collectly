import os
import tempfile

db_file = tempfile.NamedTemporaryFile()
root = os.path.dirname(os.path.dirname(__file__))


class Config(object):
    SECRET_KEY = 'REPLACE ME'


class ProdConfig(Config):
    ENV = 'prod'
    DATABASE_URI = 'sqlite:///' + os.path.join(root, 'database.db')

    CACHE_TYPE = 'simple'


class DevConfig(Config):
    ENV = 'dev'
    DEBUG = True
    DEBUG_TB_INTERCEPT_REDIRECTS = False

    DATABASE_URI = 'sqlite:///' + os.path.join(root, 'database.db')

    CACHE_TYPE = 'null'
    ASSETS_DEBUG = True


class TestConfig(Config):
    ENV = 'test'
    DEBUG = True
    DEBUG_TB_INTERCEPT_REDIRECTS = False

    DATABASE_URI = 'sqlite:///' + db_file.name
    DATABASE_ECHO = True

    CACHE_TYPE = 'null'
