#!/usr/bin/env python
import os
import sys
import platform
from subprocess import Popen, PIPE
import signal


class Server:
    def __init__(self):

        # set system/version dependent "start_new_session" analogs
        kwargs = {}
        if platform.system() == 'Windows':
            # from msdn [1]
            create_new_process_group = 0x00000200  # note: could get it from subprocess
            detached_process = 0x00000008  # 0x8 | 0x200 == 0x208
            kwargs.update(creationflags=detached_process | create_new_process_group)
        elif sys.version_info < (3, 2):  # assume posix
            kwargs.update(preexec_fn=os.setsid)
        else:  # Python 3.2+ and Unix
            kwargs.update(start_new_session=True)
        path = "python " + os.path.dirname(os.path.abspath(__file__)) + "\\run.py"
        print(path)

        self.p = Popen(path, stdin=PIPE, stdout=PIPE, stderr=PIPE, **kwargs)
        assert not self.p.poll()

    def stop(self):
        """

        :return:
        """
        p = self.p
        if platform.system() == 'Windows':
            os.kill(p.pid, signal.CTRL_C_EVENT)
        else:
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)

    def status(self):
        """

        :return:
        """
        p = self.p
        print(p.pid)
        print(p)
