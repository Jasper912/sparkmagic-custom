# -*- coding: UTF-8 -*-
# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from threading import Thread
import requests
import getpass
import re
from ipykernel.ipkernel import IPythonKernel
from hdijupyterutils.ipythondisplay import IpythonDisplay
from sparkmagic.livyclientlib.sparkcontroller import SparkController
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME
from sparkmagic.livyclientlib.exceptions import wrap_unexpected_exceptions
from sparkmagic.kernels.wrapperkernel.usercodeparser import UserCodeParser
from tornado import gen
from ipykernel.jsonutil import json_clean
from IPython.core.completer import provisionalcompleter
from IPython.core.completer import position_to_cursor
from sparkmagic.livyclientlib.endpoint import build_endpoint


class SparkKernelBase(IPythonKernel):
    def __init__(self, implementation, implementation_version, language, language_version, language_info,
                 session_language, user_code_parser=None, **kwargs):
        # Required by Jupyter - Override
        self.implementation = implementation
        self.implementation_version = implementation_version
        self.language = language
        self.language_version = language_version
        self.language_info = language_info

        # Override
        self.session_language = session_language

        super(SparkKernelBase, self).__init__(**kwargs)

        self.logger = SparkLog(u"{}_jupyter_kernel".format(self.session_language))
        self._fatal_error = None
        self.ipython_display = IpythonDisplay()
        self.spark_controller = SparkController(self.ipython_display)
        if user_code_parser is None:
            self.user_code_parser = UserCodeParser()
        else:
            self.user_code_parser = user_code_parser

        # Disable warnings for test env in HDI
        requests.packages.urllib3.disable_warnings()

        if not kwargs.get("testing", False):
            self._load_magics_extension()
            self._change_language()
            # 项目启动的时候初始化sparkmagic.magic 和session
            self._load_spark_magics_extension()
            self._init_livy_session()
            if conf.use_auto_viz():
                self._register_auto_viz()

    def _is_sql_filter(self, code):
        if conf.is_sql_restrict():
            if re.search(r'\s*show\s+databases', code.lower()):
                return True

            if re.search(r'\s*use\s+', code.lower()):
                return True

        return False

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        def f(self):
            if self._is_sql_filter(code):
                self.ipython_display.write("已为您选择好专属数据库, 直接使用show tables 试试看")
                return self._complete_cell()

            if self._fatal_error is not None:
                return self._repeat_fatal_error()

            return self._do_execute(code, silent, store_history, user_expressions, allow_stdin)
        return wrap_unexpected_exceptions(f, self._complete_cell)(self)

    def do_shutdown(self, restart):
        # Cleanup
        self._delete_session()

        return self._do_shutdown_ipykernel(restart)

    def _do_execute(self, code, silent, store_history, user_expressions, allow_stdin):
        code_to_run = self.user_code_parser.get_code_to_run(code)

        res = self._execute_cell(code_to_run, silent, store_history, user_expressions, allow_stdin)

        return res

    def _load_magics_extension(self):
        register_magics_code = "%load_ext sparkmagic.kernels"
        self._execute_cell(register_magics_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to load the Spark kernels magics library.")
        self.logger.debug("Loaded magics.")

    def _load_spark_magics_extension(self):
        '''
            初始化spark.magic，类似执行%load_ext sparkmagic.magics
        :return:
        '''
        register_spark_magics_code = "%load_ext sparkmagic.magics"
        self._execute_cell(register_spark_magics_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to load the Spark Magics library.")
        self.logger.debug("Loaded sparkmagic.magics")

    def _change_language(self):
        register_magics_code = "%%_do_not_call_change_language -l {}\n ".format(self.session_language)
        self._execute_cell(register_magics_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to change language to {}.".format(self.session_language))
        self.logger.debug("Changed language.")

    def _init_livy_session(self):
        '''
            初始化session不应该在此类执行具体操作，应该委派kernelmagics初始化session，
        :return:
        '''
        register_magics_code = "%%_do_not_call_init_livy_session -i {}\n ".format(self.session_language)
        self._execute_cell(register_magics_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to init livy session: {}.".format(self.session_language))
        self.logger.debug("Init livy session.")

    def _register_auto_viz(self):
        from sparkmagic.utils.sparkevents import get_spark_events_handler
        import autovizwidget.utils.configuration as c

        handler = get_spark_events_handler()
        c.override("events_handler", handler)

        register_auto_viz_code = """from autovizwidget.widget.utils import display_dataframe
ip = get_ipython()
ip.display_formatter.ipython_display_formatter.for_type_by_name('pandas.core.frame', 'DataFrame', display_dataframe)"""
        self._execute_cell(register_auto_viz_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to register auto viz for notebook.")
        self.logger.debug("Registered auto viz.")

    def _delete_session(self):
        code = "%%_do_not_call_delete_session\n "
        self._execute_cell_for_user(code, True, False)

    def _execute_cell(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False,
                      shutdown_if_error=False, log_if_error=None):
        reply_content = self._execute_cell_for_user(code, silent, store_history, user_expressions, allow_stdin)

        if shutdown_if_error and reply_content[u"status"] == u"error":
            error_from_reply = reply_content[u"evalue"]
            if log_if_error is not None:
                message = "{}\nException details:\n\t\"{}\"".format(log_if_error, error_from_reply)
                return self._abort_with_fatal_error(message)

        return reply_content

    def _execute_cell_for_user(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        return super(SparkKernelBase, self).do_execute(code, silent, store_history, user_expressions, allow_stdin)

    def _do_shutdown_ipykernel(self, restart):
        return super(SparkKernelBase, self).do_shutdown(restart)

    def _complete_cell(self):
        """A method that runs a cell with no effect. Call this and return the value it
        returns when there's some sort of error preventing the user's cell from executing; this
        will register the cell from the Jupyter UI as being completed."""
        return self._execute_cell("None", False, True, None, False)

    def _show_user_error(self, message):
        self.logger.error(message)
        self.ipython_display.send_error(message)

    def _queue_fatal_error(self, message):
        """Queues up a fatal error to be thrown when the next cell is executed; does not
        raise an error immediately. We use this for errors that happen on kernel startup,
        since IPython crashes if we throw an exception in the __init__ method."""
        self._fatal_error = message

    def _abort_with_fatal_error(self, message):
        """Queues up a fatal error and throws it immediately."""
        self._queue_fatal_error(message)
        return self._repeat_fatal_error()

    def _repeat_fatal_error(self):
        """Throws an error that has already been queued."""
        error = conf.fatal_error_suggestion().format(self._fatal_error)
        self.logger.error(error)
        self.ipython_display.send_error(error)
        return self._complete_cell()

    @gen.coroutine
    def complete_request(self, stream, ident, parent):
        content = parent['content']
        code = content['code']
        cursor_pos = content['cursor_pos']

        matches = yield gen.maybe_future(self.do_complete(code, cursor_pos))
        matches = json_clean(matches)
        completion_msg = self.session.send(stream, 'complete_reply',
                                           matches, parent, ident)

    def _experimental_do_complete(self, code, cursor_pos):
        """
        Experimental completions from IPython, using livy completion.
        """

        code = code.strip()
        if cursor_pos is None:
            cursor_pos = len(code)

        matches = []
        with provisionalcompleter():
            session_name = self.spark_controller.generate_livy_session_name(id(self))

            endpoint = build_endpoint(self.session_language)
            session_info_list = self.spark_controller.get_all_sessions_endpoint(endpoint)
            session_id = None
            for session in session_info_list:
                if session.session_name == session_name:
                    session_id = session.id

            if session_id:
                # Only complete the cursor_line
                cursor_line, cursor_column = position_to_cursor(code, cursor_pos)
                lines = code.split("\n")
                completion_line = lines[cursor_line]
                before_lines = lines[:cursor_line]
                if len(lines) > 1 and cursor_line > 0:
                    real_cursor_pos = cursor_pos - len("\n".join(before_lines)) - 1
                else:
                    real_cursor_pos = cursor_pos

                http_client = self.spark_controller._http_client(endpoint)
                kind = conf.get_livy_kind(self.session_language)
                res_completions = http_client.post_completion(session_id, kind, completion_line, real_cursor_pos)
                matches = res_completions.get("candidates", [])

        if matches:
            s = self.__get_cursor_start(code, cursor_pos, matches[0])
        else:
            s = cursor_pos

        res = {
            'matches': matches,
            'cursor_end': cursor_pos,
            'cursor_start': s,
            'metadata': {},
            'status': 'ok'
        }
        return res

    def __get_cursor_start(self, code, cursor_pos, match):
        before_code = code[:cursor_pos]
        before_code_rev = before_code[::-1]
        bucket = []
        for c in before_code_rev:
            if len(bucket) >= len(match):
                break

            if re.match(r"\w", c):
                bucket.insert(0, c)
            else:
                break

            if c == match[0]:
                bucket_len = len(bucket)
                completion_match_prefix = "".join(bucket)
                if completion_match_prefix == match[:bucket_len]:
                    return cursor_pos - bucket_len

        return cursor_pos

    def do_apply(self, content, bufs, msg_id, reply_metadata):
        from sparkmagic.messages_api.apply_request import ApplyRequestHandler
        result_buf = []
        reply_content = ApplyRequestHandler(self).dispath_request(content)
        return reply_content, result_buf
