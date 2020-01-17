# Distributed under the terms of the Modified BSD License.
import copy
import sys
import base64
import getpass
import requests
from hdijupyterutils.constants import EVENTS_HANDLER_CLASS_NAME, LOGGING_CONFIG_CLASS_NAME
from hdijupyterutils.utils import join_paths
from hdijupyterutils.configuration import override as _override
from hdijupyterutils.configuration import override_all as _override_all
from hdijupyterutils.configuration import with_override

from .constants import HOME_PATH, CONFIG_FILE, MAGICS_LOGGER_NAME, LIVY_KIND_PARAM, \
    LANG_SCALA, LANG_PYTHON, LANG_PYTHON3, LANG_R, LANG_SQL, \
    SESSION_KIND_SPARKR, SESSION_KIND_SPARK, SESSION_KIND_PYSPARK, SESSION_KIND_PYSPARK3, CONFIGURABLE_RETRY, \
    SESSION_CONF_PARAM, SPARK_YARN_QUEUE_PARAM, SESSION_KIND_SQL
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
import sparkmagic.utils.constants as constants


d = {}
path = join_paths(HOME_PATH, CONFIG_FILE)


def override(config, value):
    _override(d, path, config, value)


def override_all(obj):
    _override_all(d, obj)


_with_override = with_override(d, path)


# Helpers

def get_livy_kind(language):
    if language == LANG_SCALA:
        return SESSION_KIND_SPARK
    elif language == LANG_PYTHON:
        return SESSION_KIND_PYSPARK
    elif language == LANG_PYTHON3:
        # Starting with version 0.5.0-incubating, session kind “pyspark3” is removed,
        # instead users require to set PYSPARK_PYTHON to python3 executable.
        return SESSION_KIND_PYSPARK
    elif language == LANG_R:
        return SESSION_KIND_SPARKR
    elif language == LANG_SQL:
        return SESSION_KIND_SQL
    else:
        raise BadUserConfigurationException("Cannot get session kind for {}.".format(language))


def get_auth_value(username, password):
    if username == '' and password == '':
        return constants.NO_AUTH

    return constants.AUTH_BASIC


# Configs


def get_session_properties(language):
    properties = copy.deepcopy(session_configs())
    properties[LIVY_KIND_PARAM] = get_livy_kind(language)
    pyspark_extra_conf = get_pyspark_extra_conf(language)
    properties[SESSION_CONF_PARAM].update(pyspark_extra_conf)

    user_yarn_conf = get_user_yarn_queue_conf()
    properties[SESSION_CONF_PARAM].update(user_yarn_conf)

    return properties


@_with_override
def session_configs():
    return {}


@_with_override
def kernel_python_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998', u'auth': constants.NO_AUTH}


def base64_kernel_python_credentials():
    return repack_kernel_credentials(_credentials_override(kernel_python_credentials))


# No one's gonna use pyspark and pyspark3 notebook on different endpoints. Reuse the old config.
@_with_override
def kernel_python3_credentials():
    return kernel_python_credentials()


def base64_kernel_python3_credentials():
    return base64_kernel_python_credentials()


@_with_override
def kernel_scala_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998', u'auth': constants.NO_AUTH}


def base64_kernel_scala_credentials():
    return repack_kernel_credentials(_credentials_override(kernel_scala_credentials))

@_with_override
def kernel_r_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998', u'auth': constants.NO_AUTH}


def base64_kernel_r_credentials():
    return repack_kernel_credentials(_credentials_override(kernel_r_credentials))

@_with_override
def kernel_sql_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998', u'auth': constants.NO_AUTH}


def base64_kernel_sql_credentials():
    return repack_kernel_credentials(_credentials_override(kernel_sql_credentials))


def repack_kernel_credentials(credentials):
    livy_url = get_user_livy_url()
    if livy_url:
        credentials['url'] = livy_url
    return credentials


@_with_override
def logging_config():
    return {
        u"version": 1,
        u"formatters": {
            u"magicsFormatter": {
                u"format": u"%(asctime)s\t%(levelname)s\t%(message)s",
                u"datefmt": u""
            }
        },
        u"handlers": {
            u"magicsHandler": {
                u"class": LOGGING_CONFIG_CLASS_NAME,
                u"formatter": u"magicsFormatter",
                u"home_path": HOME_PATH
            }
        },
        u"loggers": {
            MAGICS_LOGGER_NAME: {
                u"handlers": [u"magicsHandler"],
                u"level": u"DEBUG",
                u"propagate": 0
            }
        }
    }


@_with_override
def events_handler_class():
    return EVENTS_HANDLER_CLASS_NAME


@_with_override
def wait_for_idle_timeout_seconds():
    return 15


@_with_override
def livy_session_startup_timeout_seconds():
    return 60


@_with_override
def fatal_error_suggestion():
    return u"""The code failed because of a fatal error:
\t{}.

Some things to try:
a) Make sure Spark has enough available resources for Jupyter to create a Spark context.
b) Contact your Jupyter administrator to make sure the Spark magics library is configured correctly.
c) Restart the kernel."""


@_with_override
def resource_limit_mitigation_suggestion():
    return ""


@_with_override
def ignore_ssl_errors():
    return False


@_with_override
def coerce_dataframe():
    return True


@_with_override
def use_auto_viz():
    return True


@_with_override
def default_maxrows():
    return 2500


@_with_override
def default_samplemethod():
    return "take"


@_with_override
def default_samplefraction():
    return 0.1


@_with_override
def pyspark_dataframe_encoding():
    return u'utf-8'


@_with_override
def heartbeat_refresh_seconds():
    return 30


@_with_override
def heartbeat_retry_seconds():
    return 10


@_with_override
def livy_server_heartbeat_timeout_seconds():
    return 0


@_with_override
def server_extension_default_kernel_name():
    return "pysparkkernel"


@_with_override
def custom_headers():
    return {}


@_with_override
def retry_policy():
    return CONFIGURABLE_RETRY


@_with_override
def retry_seconds_to_sleep_list():
    return [0.2, 0.5, 1, 3, 5]


@_with_override
def configurable_retry_policy_max_retries():
    # Sum of default values is ~10 seconds.
    # Plus 15 seconds more wanted, that's 3 more 5 second retries.
    return 8

@_with_override
def switch_to_user_database():
    return False

@_with_override
def user_database():
    return None

@_with_override
def is_sql_restrict():
    return True

def _credentials_override(f):
    """Provides special handling for credentials. It still calls _override().
    If 'base64_password' in config is set, it will base64 decode it and returned in return value's 'password' field.
    If 'base64_password' is not set, it will fallback to 'password' in config.
    """
    credentials = f()
    base64_decoded_credentials = {k: credentials.get(k) for k in ('username', 'password', 'url', 'auth')}
    base64_password = credentials.get('base64_password')
    if base64_password is not None:
        try:
            base64_decoded_credentials['password'] = base64.b64decode(base64_password).decode()
        except Exception:
            exception_type, exception, traceback = sys.exc_info()
            msg = "base64_password for %s contains invalid base64 string: %s %s" % (f.__name__, exception_type, exception)
            raise BadUserConfigurationException(msg)
    if base64_decoded_credentials['auth'] is None:
        base64_decoded_credentials['auth'] = get_auth_value(base64_decoded_credentials['username'], base64_decoded_credentials['password'])
    return base64_decoded_credentials


def get_pyspark_extra_conf(language):
    if language == LANG_PYTHON:
        return pyspark_extra_conf()

    elif language == LANG_PYTHON3:
        return pyspark3_extra_conf()

    else:
        return {}

@_with_override
def pyspark_extra_conf():
    conf = {
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/usr/local/bin/python2.7",
        "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python2.7",
        "spark.executorEnv.PYSPARK_PYTHON": "/usr/local/bin/python2.7"
    }
    return conf

@_with_override
def pyspark3_extra_conf():
    conf = {
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/usr/local/bin/python3.6",
        "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3.6",
        "spark.executorEnv.PYSPARK_PYTHON": "/usr/local/bin/python3.6",
        "spark.executorEnv.PYTHONPATH": "/usr/local/anaconda3/lib/python3.6/site-packages"
    }
    return conf


@_with_override
def config_api():
    return "http://127.0.0.1"


@_with_override
def config_api_auth_token():
    return ""


def get_user_config():
    username = getpass.getuser()
    api = config_api()
    try:
        url = f"{api}/user/yarn/{username}"
        headers = {"Authorization": "Bearer {}".format(config_api_auth_token())}

        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            result = res.json()
            if result['code'] == 0:
                data = result.get("data", {})
                return data
    except:
        return {}

    return {}


def get_user_yarn_queue_conf():
    config = get_user_config()
    yarn_queue = config.get("yarn_queue", "")
    conf = {
        SPARK_YARN_QUEUE_PARAM: yarn_queue
    }
    return conf


def get_user_livy_url():
    config = get_user_config()
    return config.get("livy_url", "")
