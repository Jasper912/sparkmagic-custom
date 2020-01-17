# -*- coding: UTF-8 -*-

"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from six import string_types
from IPython.core.magic import Magics, magics_class
from hdijupyterutils.ipythondisplay import IpythonDisplay
import getpass
import sparkmagic.utils.constants as constants
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.utils import get_sessions_info_html, convert_data_struct_to_dataframe
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME
from sparkmagic.livyclientlib.sparkcontroller import SparkController
from sparkmagic.livyclientlib.sqlquery import SQLQuery
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.sparkstorecommand import SparkStoreCommand
from sparkmagic.livyclientlib.endpoint import build_endpoint

@magics_class
class SparkMagicBase(Magics):
    def __init__(self, shell, data=None, spark_events=None):
        # You must call the parent constructor
        super(SparkMagicBase, self).__init__(shell)

        self.logger = SparkLog(u"SparkMagics")
        self.ipython_display = IpythonDisplay()
        self.spark_controller = SparkController(self.ipython_display)

        self.logger.debug("Initialized spark magics.")

        if spark_events is None:
            spark_events = SparkEvents()
        spark_events.emit_library_loaded_event()

    def _get_session_name_by_session(self, session):
        session_name = self.spark_controller.session_manager.get_session_name_by_id(session.id)
        # 如果session不存在,则将session激活并加入session_list
        if not session_name :
            session_name = session.session_name
            if session_name:
                self.spark_controller.session_manager.add_session(session_name, session)
                session.already_start()
                return session_name
        else:
            return session_name

        return None

    def init_livy_session(self, language="python"):
        '''
            执行sql时自动初始化sql
        :return:
        '''
        return self.__get_or_create_session(language)

    def __get_or_create_session(self, language):
        proxy_user = getpass.getuser()

        self.session_language = language
        endpoint = build_endpoint(self.session_language)
        kernel_instance_id = id(self.shell.kernel)
        session_name_seleted = self.spark_controller.generate_livy_session_name(kernel_instance_id)

        properties = conf.get_session_properties(self.session_language)
        properties["proxyUser"] = proxy_user
        properties["session_language"] = self.session_language
        properties["session_name"] = session_name_seleted

        session_info_list = self.spark_controller.get_all_sessions_endpoint(endpoint)
        for session in session_info_list:
            # session kind 必须一致
            if session.kind != properties['kind']:
                continue

            # 区分pyspark 及 pyspark3
            if session.session_language != properties['session_language']:
                continue

            session_name = self._get_session_name_by_session(session)
            if session_name == session_name_seleted:
                if session.status in constants.HEALTHY_SESSION_STATUS:
                    return session_name_seleted
                elif session.status in constants.FINAL_STATEMENT_STATUS:
                    # FINAL, recreate new session
                    self.spark_controller.add_session(session_name_seleted, endpoint, False, properties)
                    return session_name_seleted
        else:
            # 如果livy中没有session，则创建session
            self.spark_controller.add_session(session_name_seleted, endpoint, False, properties)
            return session_name_seleted

    def execute_spark(self, cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce):
        (success, out) = self.spark_controller.run_command(Command(cell), session_name)
        if not success:
            self.ipython_display.send_error(out)
        else:
            if isinstance(out, string_types):
                self.ipython_display.write(out)
            elif isinstance(out, dict):
                df = convert_data_struct_to_dataframe(out)
                html = df.fillna('NULL').astype(str).to_html(notebook=True)
                self.ipython_display.html(html)
            else:
                self.ipython_display.write(out)
            if output_var is not None:
                spark_store_command = self._spark_store_command(output_var, samplemethod, maxrows, samplefraction, coerce)
                df = self.spark_controller.run_command(spark_store_command, session_name)
                self.shell.user_ns[output_var] = df

    @staticmethod
    def _spark_store_command(output_var, samplemethod, maxrows, samplefraction, coerce):
        return SparkStoreCommand(output_var, samplemethod, maxrows, samplefraction, coerce=coerce)

    def execute_sqlquery(self, cell, samplemethod, maxrows, samplefraction,
                         session, output_var, quiet, coerce):
        sqlquery = self._sqlquery(cell, samplemethod, maxrows, samplefraction, coerce)
        df = self.spark_controller.run_sqlquery(sqlquery, session)
        if output_var is not None:
            self.shell.user_ns[output_var] = df
        if quiet:
            return None
        else:
            return df

    @staticmethod
    def _sqlquery(cell, samplemethod, maxrows, samplefraction, coerce):
        return SQLQuery(cell, samplemethod, maxrows, samplefraction, coerce=coerce)

    def _print_endpoint_info(self, info_sessions, current_session_id):
        if info_sessions:
            info_sessions = sorted(info_sessions, key=lambda s: s.id)
            html = get_sessions_info_html(info_sessions, current_session_id)
            self.ipython_display.html(html)
        else:
            self.ipython_display.html(u'No active sessions.')
