import os
import unittest
import tempfile
import fcntl
import time
import shutil

import smartdispatch.utils as utils
from smartdispatch.command_manager import CommandManager

from subprocess import Popen, call, PIPE

from nose.tools import assert_true, assert_equal


class TestSmartWorker(unittest.TestCase):

    def setUp(self):
        self.commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        self._commands_dir = tempfile.mkdtemp()
        self.logs_dir = tempfile.mkdtemp()

        self.command_manager = CommandManager(os.path.join(self._commands_dir, "commands.txt"))
        self.command_manager.set_commands_to_run(self.commands)

        self.commands_uid = map(utils.generate_uid_from_string, self.commands)

    def tearDown(self):
        shutil.rmtree(self._commands_dir)
        shutil.rmtree(self.logs_dir)

    def test_main(self):
        command = ["smart_worker.py", self.command_manager._commands_filename, self.logs_dir]
        assert_equal(call(command), 0)

        # Check output logs
        filenames = os.listdir(self.logs_dir)
        outlogs = [os.path.join(self.logs_dir, filename) for filename in filenames if filename.endswith(".out")]
        for log_filename in outlogs:
            with open(log_filename) as logfile:
                # From log's filename (i.e. uid) retrieve executed command associated with this log
                uid = os.path.splitext(os.path.basename(log_filename))[0]
                executed_command = self.commands[self.commands_uid.index(uid)]

                # First line is the executed command in comment
                header = logfile.readline().strip()
                assert_equal(header, "# " + executed_command)

                # Next should be the command's output
                output = logfile.readline().strip()
                assert_equal(output, executed_command[-1])  # We know those are 'echo' of a digit

                # Log should be empty now
                assert_equal("", logfile.read())

        # Check error logs
        errlogs = [os.path.join(self.logs_dir, filename) for filename in filenames if filename.endswith(".err")]
        for log_filename in errlogs:
            with open(log_filename) as logfile:
                # From log's filename (i.e. uid) retrieve executed command associated with this log
                uid = os.path.splitext(os.path.basename(log_filename))[0]
                executed_command = self.commands[self.commands_uid.index(uid)]

                # First line is the executed command in comment
                header = logfile.readline().strip()
                assert_equal(header, "# " + executed_command)

                # Log should be empty now
                assert_equal("", logfile.read())

    def test_lock(self):
        command = ["smart_worker.py", self.command_manager._commands_filename, self.logs_dir]

        # Lock the commands file before running 'smart_worker.py'
        with open(self.command_manager._commands_filename) as commands_file:
            fcntl.flock(commands_file.fileno(), fcntl.LOCK_EX)
            process = Popen(command, stdout=PIPE, stderr=PIPE)
            time.sleep(0.1)
            fcntl.flock(commands_file.fileno(), fcntl.LOCK_UN)

        stdout, stderr = process.communicate()
        assert_equal(stdout, "")
        assert_true("write-lock" in stderr, msg="Forcing a race condition, try increasing sleeping time above.")
