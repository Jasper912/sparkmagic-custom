# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.utils.constants import LANG_SQL
from sparkmagic.kernels.wrapperkernel.sparkkernelbase import SparkKernelBase


class SparkSQLKernel(SparkKernelBase):
    def __init__(self, **kwargs):
        implementation = 'SparkSQL'
        implementation_version = '1.0'
        language = 'no-op'
        language_version = '0.1'
        language_info = {
            'name': 'sql',
            'mimetype': 'application/json',
            'codemirror_mode': 'application/json',
            'pygments_lexer': 'sql'
        }

        session_language = LANG_SQL

        super(SparkSQLKernel, self).__init__(implementation, implementation_version, language, language_version,
                                        language_info, session_language, **kwargs)


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=SparkSQLKernel)
