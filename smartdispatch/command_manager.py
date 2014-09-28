import os
from smartdispatch import utils


def append_command_to_file(commands_filename, command):
    with utils.open_with_lock(commands_filename, 'a') as commands_file:
        commands_file.writelines(command)


def move_running_command_to_finished(running_commands_filename, finished_commands_filename, command):
    with utils.open_with_lock(running_commands_filename, 'r+') as running_commands_file:
        running_commands = running_commands_file.readlines()
        running_commands.remove(command)

        running_commands_file.seek(0, os.SEEK_SET)
        running_commands_file.writelines(running_commands)
        running_commands_file.truncate()

        append_command_to_file(finished_commands_filename, command)
