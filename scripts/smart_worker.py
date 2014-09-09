#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import fcntl
import argparse
import subprocess

import smartdispatch.utils as utils


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('commands_filename', type=str, help='File containing all commands to execute.')
    parser.add_argument('logs_dir', type=str, help="Folder where to put commands' stdout and stderr.")
    args = parser.parse_args()

    # Check for invalid arguments
    if not os.path.isfile(args.commands_filename):
        parser.error("You need to specify the name of the file containing the commands.")

    if not os.path.isdir(args.logs_dir):
        parser.error("You need to specify the folder path where to put command' stdout and stderr.")

    return args


def main():
    args = parse_arguments()

    while True:
        with open(args.commands_filename, 'rw+') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            command = f.readline().strip()
            remaining = f.read()
            f.seek(0, os.SEEK_SET)
            f.write(remaining)
            f.truncate()

        if command == '':
            break

        uid = utils.generate_uid_from_command(command)
        stdout_filename = os.path.join(args.logs_dir, uid + ".out")
        stderr_filename = os.path.join(args.logs_dir, uid + ".err")

        with open(stdout_filename, 'w') as stdout:
            with open(stderr_filename, 'w') as stderr:
                stdout.write("# " + command + '\n')
                stderr.write("# " + command + '\n')
                stdout.flush()
                stderr.flush()
                subprocess.call(command, stdout=stdout, stderr=stderr, shell=True)


if __name__ == '__main__':
    main()
