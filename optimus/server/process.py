import os
import platform
import signal
import sys
from subprocess import Popen, PIPE

import psutil

WINDOWS = "windows"
PLATFORM = platform.system().lower()


# test https://stackoverflow.com/questions/984941/python-subprocess-popen-from-a-thread

class Process:
    """
    A helper class to start and stop process on windows/unix systems
    """

    def __init__(self, path=None):

        # set system/version dependent "start_new_session" analogs
        kwargs = {}
        if PLATFORM == WINDOWS:
            # from msdn [1]
            create_new_process_group = 0x00000200  # note: could get it from subprocess
            detached_process = 0x00000008  # 0x8 | 0x200 == 0x208
            kwargs.update(creationflags=detached_process | create_new_process_group)
        elif sys.version_info < (3, 2):  # assume posix
            kwargs.update(preexec_fn=os.setsid)
        else:  # Python 3.2+ and Unix
            kwargs.update(start_new_session=True)

        process = Popen(path, stdin=PIPE, stdout=PIPE, stderr=PIPE, **kwargs)

        # Ensure that a child process has completed before the main process
        # process.join()

        self.process = process
        self.path = path

    def stop(self):
        """

        :return:
        """
        self.stop_id(self.id)

    # TODO: Maybe this should be outside Process()
    @staticmethod
    def stop_id(pid):
        """
        Stop the process that start the server
        :return:
        """

        def kill_proc(pid):
            """
            Kill process
            :param pid:
            :return:
            """
            parent = psutil.Process(pid)
            parent.kill()

        if PLATFORM == WINDOWS:
            kill_proc(pid)
        else:
            os.killpg(os.getpgid(pid), signal.SIGTERM)

    def status(self):
        """
        Return the process status
        :return:
        """
        return self.process

    @property
    def id(self):
        """
        Return the process id
        :return:
        """
        return self.process.pid
