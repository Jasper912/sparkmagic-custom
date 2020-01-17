# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import sparkmagic.utils.configuration as conf
import sparkmagic.utils.constants as constants
from sparkmagic.utils.sparklogger import SparkLog
from .sessionmanager import SessionManager
from .livyreliablehttpclient import LivyReliableHttpClient
from .livysession import LivySession
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME, SESSION_CONF_PARAM, SPARK_YARN_QUEUE_PARAM
from sparkmagic.livyclientlib.endpoint import build_endpoint


class SparkController(object):
    def __init__(self, ipython_display):
        self.logger = SparkLog(u"SparkController")
        self.ipython_display = ipython_display
        self.session_manager = SessionManager()

    def get_app_id(self, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return session_to_use.get_app_id()

    def get_driver_log_url(self, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return session_to_use.get_driver_log_url()

    def get_logs(self, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return session_to_use.get_logs()

    def get_spark_ui_url(self, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return session_to_use.get_spark_ui_url()

    def run_command(self, command, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return command.execute(session_to_use)

    def run_sqlquery(self, sqlquery, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return sqlquery.execute(session_to_use)

    def get_all_sessions_endpoint(self, endpoint):
        http_client = self._http_client(endpoint)
        sessions = http_client.get_sessions()[u"sessions"]

        # 只获取当前用户的sessions
        import getpass
        current_user = getpass.getuser()
        _sessions = [s for s in sessions if s['proxyUser'] == current_user]
        _sessions.reverse()
        session_list = [self._livy_session(http_client, {constants.LIVY_KIND_PARAM: s[constants.LIVY_KIND_PARAM]},
                                           self.ipython_display, s[u"id"])
                        for s in _sessions]

        for s in session_list:
            s.refresh_status_and_info()
        return session_list

    def get_all_sessions_endpoint_info(self, endpoint):
        sessions = self.get_all_sessions_endpoint(endpoint)
        return [str(s) for s in sessions]

    def cleanup(self):
        self.session_manager.clean_up_all()

    def cleanup_endpoint(self, endpoint):
        for session in self.get_all_sessions_endpoint(endpoint):
            session.delete()

    def delete_session_by_name(self, name):
        self.session_manager.delete_client(name)

    def delete_session_by_id(self, endpoint, session_id):
        name = self.session_manager.get_session_name_by_id_endpoint(session_id, endpoint)

        if name in self.session_manager.get_sessions_list():
            self.delete_session_by_name(name)
        else:
            http_client = self._http_client(endpoint)
            response = http_client.get_session(session_id)
            http_client = self._http_client(endpoint)
            session = self._livy_session(http_client, {constants.LIVY_KIND_PARAM: response[constants.LIVY_KIND_PARAM]},
                                        self.ipython_display, session_id)
            session.delete()

    def add_session(self, name, endpoint, skip_if_exists, properties):
        if skip_if_exists and (name in self.session_manager.get_sessions_list()):
            self.logger.debug(u"Skipping {} because it already exists in list of sessions.".format(name))
            return
        http_client = self._http_client(endpoint)
        session = self._livy_session(http_client, properties, self.ipython_display)
        self.session_manager.add_session(name, session)
        session.start()
        # switch user databases
        self.switch_user_database(name)

    def switch_user_database(self, session_name):
        from sparkmagic.livyclientlib.sqlquery import SQLQuery
        database = conf.user_database()
        if conf.switch_to_user_database() and database:
            switch_db_code = "use {}".format(database)
            sqlquery = SQLQuery(switch_db_code)
            self.run_sqlquery(sqlquery, session_name)
            self.logger.debug("Switch user database: %s" % database)

    def get_session_id_for_client(self, name):
        return self.session_manager.get_session_id_for_client(name)

    def get_client_keys(self):
        return self.session_manager.get_sessions_list()

    def get_manager_sessions_str(self):
        return self.session_manager.get_sessions_info()

    def get_session_by_name_or_default(self, client_name):
        if client_name is None:
            return self.session_manager.get_any_session()
        else:
            client_name = client_name.lower()
            return self.session_manager.get_session(client_name)

    def get_managed_clients(self):
        return self.session_manager.sessions

    @staticmethod
    def _livy_session(http_client, properties, ipython_display,
                      session_id=-1):
        return LivySession(http_client, properties, ipython_display,
                           session_id, heartbeat_timeout=conf.livy_server_heartbeat_timeout_seconds())

    @staticmethod
    def _http_client(endpoint):
        return LivyReliableHttpClient.from_endpoint(endpoint)

    def create_livy_session(self, kernel_instance_id, session_language, proxy_user, yarn_queue=None):
        endpoint = build_endpoint(session_language)
        session_name = self.generate_livy_session_name(kernel_instance_id)

        properties = conf.get_session_properties(session_language)
        properties["proxyUser"] = proxy_user
        properties["session_language"] = session_language
        properties["session_name"] = session_name
        if yarn_queue:
            properties[SESSION_CONF_PARAM][SPARK_YARN_QUEUE_PARAM] = yarn_queue

        self.add_session(session_name, endpoint, False, properties)
        return session_name

    def generate_livy_session_name(self, kernel_instance_id):
        return u"session_name-{}".format(kernel_instance_id)
