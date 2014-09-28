import os
import unittest

from smartdispatch import command_manager
from nose.tools import assert_equal


class CommandFilesTests(unittest.TestCase):

    def setUp(self):
        self.running_commands_filename = "running_commands.txt"
        self.finished_commands_filename = "finished_commands.txt"
        self.command1 = "1\n"
        self.command2 = "2\n"
        self.command3 = "3\n"

        with open(self.running_commands_filename, "w") as running_commands_file:
            running_commands_file.write(self.command1)

    def tearDown(self):
        try:
            os.remove(self.running_commands_filename)
        except OSError:
            pass

        try:
            os.remove(self.finished_commands_filename)
        except OSError:
            pass

    def test_append_command_to_file(self):
        # The function to test
        command_manager.append_command_to_file(self.running_commands_filename, self.command2)

        # Test validation
        with open(self.running_commands_filename, "r") as running_commands_file:
            assert_equal(running_commands_file.read(), self.command1 + self.command2)

    def test_move_running_command_to_finished(self):
        # SetUp for this test
        command_manager.append_command_to_file(self.running_commands_filename, self.command2)
        command_manager.append_command_to_file(self.running_commands_filename, self.command3)

        # The function to test
        command_manager.move_running_command_to_finished(self.running_commands_filename, self.finished_commands_filename, self.command2)

        # Test validation
        with open(self.running_commands_filename, "r") as running_commands_file:
            assert_equal(running_commands_file.read(), self.command1 + self.command3)
        with open(self.finished_commands_filename, "r") as finished_commands_file:
            assert_equal(finished_commands_file.read(), self.command2)
