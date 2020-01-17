#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2020/01/10

"""
    IPython Kernel apply_request
"""

import traceback
import getpass
import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.endpoint import build_endpoint

class ApplyRequestHandler(object):

    request_types = [
        "get_yarn_queue", "set_yarn_queue"
    ]

    def __init__(self, kernel):
        self.kernel = kernel
        self.logger = kernel.logger
        self._handlers = {}
        for request_type in self.request_types:
            self._handlers[request_type] = getattr(self, request_type)

    def dispath_request(self, msg_content):
        request_type = msg_content.get("request_type")
        reply_content = {
            'status' : 'ok',
            "request_type": request_type,
            "message": "",
            "data": {}
        }

        handler = self._handlers.get(request_type) if request_type else None
        if not handler:
            msg = "Unknown request type: %s" % request_type
            self.logger.error(msg)
            reply_content['message'] = msg
            reply_content['status'] = "error"
            return reply_content

        try:
            data = handler(msg_content)
            if data:
                if isinstance(data, (dict, list)):
                    reply_content['data'] = data
            else:
                reply_content['status'] = "error"
        except Exception as e:
            self.logger.error("ApplyRequestHandler Raise Exception: %r" % r)
            reply_content['message'] = "internel server error"
            reply_content['status'] = "error"

        return reply_content

    def set_yarn_queue(self, msg_content):
        """
            设置当前kernel所用的yarn queue，重新生成对应的livy session
        """
        yarn_queue = msg_content.get("yarn_queue", None)
        if not yarn_queue:
            return False

        # remove sessions of current kernel
        kernel_instance_id = id(self.kernel)
        session_name = self.kernel.spark_controller.generate_livy_session_name(kernel_instance_id)
        try:
            endpoint = build_endpoint(self.kernel.session_language)
            session_info_list = self.kernel.spark_controller.get_all_sessions_endpoint(endpoint)
            for session in session_info_list:
                if session.session_name == session_name:
                    self.kernel.spark_controller.delete_session_by_id(endpoint, session.id)
        except:
            pass

        # recreate session with specific yarn queue
        proxy_user = getpass.getuser()
        self.kernel.spark_controller.create_livy_session(
            kernel_instance_id, self.kernel.session_language,
            proxy_user, yarn_queue=yarn_queue
        )

        return True

    def get_yarn_queue(self, *args, **kwargs):
        """
            返回当前kernel 对应的yarn queue
        """
        kernel_instance_id = id(self.kernel)
        session_name = self.kernel.spark_controller.generate_livy_session_name(kernel_instance_id)
        endpoint = build_endpoint(self.kernel.session_language)
        session_info_list = self.kernel.spark_controller.get_all_sessions_endpoint(endpoint)
        current_yarn_queue = None
        for session in session_info_list:
            if session.session_name == session_name:
                current_yarn_queue = session.spark_yarn_queue
                break

        config = conf.get_user_config()
        if not current_yarn_queue:
            current_yarn_queue = config.get("yarn_queue", "")
        data = {
            "current": current_yarn_queue,
            "yarn_queue_list": config.get("yarn_queue_list", [])
        }
        return data
