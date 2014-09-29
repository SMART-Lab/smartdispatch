import os
import unittest
import tempfile as tmp
import shutil

from smartdispatch.command_manager import CommandManager
from nose.tools import assert_equal, assert_true


class CommandFilesTests(unittest.TestCase):

    def setUp(self):
        self._base_dir = tmp.mkdtemp()
        self.command1 = "1\n"
        self.command2 = "2\n"
        self.command3 = "3\n"
        print self._base_dir
        command_filename = os.path.join(self._base_dir, "commant.txt")

        with open(command_filename, "w+") as commands_file:
            commands_file.write(self.command1 + self.command2 + self.command3)

        self.command_manager = CommandManager(command_filename)

    def tearDown(self):
        shutil.rmtree(self._base_dir)

    def test_get_command_to_run(self):
        # The function to test
        command = self.command_manager.get_command_to_run()

        # Test validation
        assert_equal(command, self.command1)

        with open(self.command_manager._commands_filename, "r") as commands_file:
            assert_equal(commands_file.read(), self.command2 + self.command3)

        with open(self.command_manager._running_commands_filename, "r") as running_commands_file:
            assert_equal(running_commands_file.read(), self.command1)

        assert_true(not os.path.isfile(self.command_manager._finished_commands_filename))

    def test_set_running_command_as_finished(self):
        # SetUp
        command = self.command_manager.get_command_to_run()

        # The function to test
        self.command_manager.set_running_command_as_finished(command)

        # Test validation
        with open(self.command_manager._commands_filename, "r") as commands_file:
            assert_equal(commands_file.read(), self.command2 + self.command3)

        with open(self.command_manager._running_commands_filename, "r") as running_commands_file:
            assert_equal(running_commands_file.read(), "")

        with open(self.command_manager._finished_commands_filename, "r") as finished_commands_file:
            assert_equal(finished_commands_file.read(), self.command1)
