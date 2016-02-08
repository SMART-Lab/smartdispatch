import os
import unittest
import tempfile
import time
import shutil

from smartdispatch import utils
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
        # Simulate a resume, i.e. re-run the command, the output/error should be concatenated.
        self.command_manager.set_commands_to_run(self.commands)
        assert_equal(call(command), 0)

        # Check output logs
        filenames = os.listdir(self.logs_dir)
        outlogs = [os.path.join(self.logs_dir, filename) for filename in filenames if filename.endswith(".out")]
        for log_filename in outlogs:
            with open(log_filename) as logfile:
                # From log's filename (i.e. uid) retrieve executed command associated with this log
                uid = os.path.splitext(os.path.basename(log_filename))[0]
                executed_command = self.commands[self.commands_uid.index(uid)]

                # Since the command was run twice.
                for _ in range(2):
                    # First line is the datetime of the executed command in comment.
                    line = logfile.readline().strip()
                    assert_true(time.strftime("## %Y-%m-%d %H:%M:") in line)  # Don't check seconds.

                    # Second line is the executed command in comment.
                    line = logfile.readline().strip()
                    assert_equal(line, "# " + executed_command)

                    # Next should be the command's output
                    line = logfile.readline().strip()
                    assert_equal(line, executed_command[-1])  # We know those are 'echo' of a digit

                    # Empty line
                    assert_equal(logfile.readline().strip(), "")

                # Log should be empty now
                assert_equal("", logfile.read())

        # Check error logs
        errlogs = [os.path.join(self.logs_dir, filename) for filename in filenames if filename.endswith(".err")]
        for log_filename in errlogs:
            with open(log_filename) as logfile:
                # From log's filename (i.e. uid) retrieve executed command associated with this log
                uid = os.path.splitext(os.path.basename(log_filename))[0]
                executed_command = self.commands[self.commands_uid.index(uid)]

                # Since the command was run twice.
                for _ in range(2):
                    # First line is the datetime of the executed command in comment.
                    line = logfile.readline().strip()
                    assert_true(time.strftime("## %Y-%m-%d %H:%M:") in line)  # Don't check seconds.

                    # Second line is the executed command in comment.
                    line = logfile.readline().strip()
                    assert_equal(line, "# " + executed_command)

                    # Empty line
                    assert_equal(logfile.readline().strip(), "")

                # Log should be empty now
                assert_equal("", logfile.read())

    def test_lock(self):
        command = ["smart_worker.py", self.command_manager._commands_filename, self.logs_dir]

        # Lock the commands file before running 'smart_worker.py'
        with utils.open_with_lock(self.command_manager._commands_filename, 'r+'):
            process = Popen(command, stdout=PIPE, stderr=PIPE)
            time.sleep(1)

        stdout, stderr = process.communicate()
        assert_equal(stdout, "")
        assert_true("write-lock" in stderr, msg="Forcing a race condition, try increasing sleeping time above.")
        assert_true("Traceback" not in stderr)  # Check that there are no errors.
