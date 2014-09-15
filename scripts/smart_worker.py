#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import fcntl
import argparse
import subprocess
import logging

import smartdispatch.utils as utils


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('commands_filename', type=str, help='File containing all commands to execute.')
    parser.add_argument('logs_dir', type=str, help="Folder where to put commands' stdout and stderr.")
    args = parser.parse_args()

    # Check for invalid arguments
    if not os.path.isfile(args.commands_filename):
        parser.error("Invalid file path. Specify path to a file containing commands.")

    if not os.path.isdir(args.logs_dir):
        parser.error("You need to specify the folder path where to put command' stdout and stderr.")

    return args


def main():
    args = parse_arguments()

    while True:
        with open(args.commands_filename, 'rw+') as commands_file:
            try:
                fcntl.flock(commands_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                logging.info("Can't immediately write-lock the file ({0}), blocking ...".format(args.commands_filename))
                fcntl.flock(commands_file.fileno(), fcntl.LOCK_EX)

            command = commands_file.readline().strip()
            remaining = commands_file.read()
            commands_file.seek(0, os.SEEK_SET)
            commands_file.write(remaining)
            commands_file.truncate()

        if command == '':
            break

        uid = utils.generate_uid(command)
        stdout_filename = os.path.join(args.logs_dir, uid + ".out")
        stderr_filename = os.path.join(args.logs_dir, uid + ".err")

        with open(stdout_filename, 'w') as stdout_file:
            with open(stderr_filename, 'w') as stderr_file:
                stdout_file.write("# " + command + '\n')
                stderr_file.write("# " + command + '\n')
                stdout_file.flush()
                stderr_file.flush()
                subprocess.call(command, stdout=stdout_file, stderr=stderr_file, shell=True)


if __name__ == '__main__':
    main()
