#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import sys
import signal
import argparse
import subprocess
import logging
import time as t

from smartdispatch import utils
from smartdispatch.command_manager import CommandManager


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('commands_filename', type=str, help='File containing all commands to execute.')
    parser.add_argument('logs_dir', type=str, help="Folder where to put commands' stdout and stderr.")
    parser.add_argument('-r', '--assumeResumable', action='store_true', help="Assume that commands are resumable and put them into the pending list on worker termination.")
    args = parser.parse_args()

    # Check for invalid arguments
    if not os.path.isfile(args.commands_filename):
        parser.error("Invalid file path. Specify path to a file containing commands.")

    if not os.path.isdir(args.logs_dir):
        parser.error("You need to specify the folder path where to put command' stdout and stderr.")

    return args


def main():
    # Necessary if we want 'logging.info' to appear in stderr.
    logging.root.setLevel(logging.INFO)

    args = parse_arguments()

    command_manager = CommandManager(args.commands_filename)

    if args.assumeResumable:
        # Handle TERM signal gracefully by sending running commands back to
        # the list of pending commands.
        # NOTE: There are several cases when the handler will not have
        #       up-to-date information on running the command and/or process,
        #       but chances of that happening are VERY slim and the
        #       consequences are not fatal.
        def sigterm_handler(signal, frame):
            if sigterm_handler.triggered:
                return
            else:
                sigterm_handler.triggered = True
            error_code = 0
            if sigterm_handler.proc is not None:
                error_code = sigterm_handler.proc.wait()
            if sigterm_handler.command is not None:
                if error_code == 0:  # The command was terminated successfully.
                    command_manager.set_running_command_as_pending(sigterm_handler.command)
                else:
                    command_manager.set_running_command_as_finished(sigterm_handler.command, error_code)
            sys.exit(0)
        sigterm_handler.triggered = False
        sigterm_handler.command = None
        sigterm_handler.proc = None
        signal.signal(signal.SIGTERM, sigterm_handler)

    while True:
        command = command_manager.get_command_to_run()
        if args.assumeResumable:
            sigterm_handler.proc = None
            sigterm_handler.command = command

        if command is None:
            break

        uid = utils.generate_uid_from_string(command)
        stdout_filename = os.path.join(args.logs_dir, uid + ".out")
        stderr_filename = os.path.join(args.logs_dir, uid + ".err")

        # Get job and node ID
        job_id = os.environ.get('PBS_JOBID', 'undefined')
        node_name = os.environ.get('HOSTNAME', 'undefined')

        with open(stdout_filename, 'a') as stdout_file:
            with open(stderr_filename, 'a') as stderr_file:
                log_datetime = t.strftime("## SMART-DISPATCH - Started on: %Y-%m-%d %H:%M:%S - In job: {job_id} - On nodes: {node_name} ##\n".format(job_id=job_id, node_name=node_name))
                if stdout_file.tell() > 0:  # Not the first line in the log file.
                    log_datetime = t.strftime("\n## SMART-DISPATCH - Resumed on: %Y-%m-%d %H:%M:%S - In job: {job_id} - On nodes: {node_name} ##\n".format(job_id=job_id, node_name=node_name))

                log_command = "## SMART-DISPATCH - Command: " + command + '\n'

                stdout_file.write(log_datetime + log_command)
                stdout_file.flush()
                stderr_file.write(log_datetime + log_command)
                stderr_file.flush()

                proc = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file, shell=True)
                if args.assumeResumable:
                    sigterm_handler.proc = proc
                error_code = proc.wait()

        command_manager.set_running_command_as_finished(command, error_code)

if __name__ == '__main__':
    main()
