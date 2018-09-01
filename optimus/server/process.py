import os
import platform
import psutil
import signal
import sys
from subprocess import Popen, PIPE

WINDOWS = "windows"

# test https://stackoverflow.com/questions/984941/python-subprocess-popen-from-a-thread

class Process:
    """
    A helper class to start and stop process on windows/unix systems
    """

    def __init__(self, path=None):

        # set system/version dependent "start_new_session" analogs
        kwargs = {}
        if platform.system() == WINDOWS:
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
        process.join()

        self.process = process

        assert not self.process.poll()

        self.path = path

    def stop(self):
        """
        Stop the process that start the server
        :return:
        """
        process = self.process

        # Reference https://stackoverflow.com/questions/1230669/subprocess-deleting-child-processes-in-windows
        def kill_proc_tree(pid, including_parent=True):
            """
            Kill process and children
            :param pid:
            :param including_parent:
            :return:
            """
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)
            for child in children:
                child.kill()
            gone, still_alive = psutil.wait_procs(children, timeout=5)
            if including_parent:
                parent.wait(5)
                parent.kill()

        def kill_proc(pid):
            """
            Kill process
            :param pid:
            :return:
            """
            parent = psutil.Process(pid)
            parent.kill()

        if platform.system() == WINDOWS:
            kill_proc(process.pid)
        else:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)

    def status(self):
        """
        Return the process status
        :return:
        """
        return self.process
