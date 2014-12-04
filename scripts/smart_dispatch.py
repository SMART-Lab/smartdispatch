#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
import numpy as np
from subprocess import check_output

from smartdispatch.command_manager import CommandManager

from smartdispatch.queue import Queue
from smartdispatch.job_generator import job_generator_factory
from smartdispatch import get_available_queues
from smartdispatch import utils

import logging
import smartdispatch


LOGS_FOLDERNAME = "SMART_DISPATCH_LOGS"
CLUSTER_NAME = utils.detect_cluster()
AVAILABLE_QUEUES = get_available_queues(CLUSTER_NAME)
LAUNCHER = utils.get_launcher(CLUSTER_NAME)


def main():
    # Necessary if we want 'logging.info' to appear in stderr.
    logging.root.setLevel(logging.INFO)

    args = parse_arguments()

    # Check if RESUME or LAUNCH mode
    if args.mode == "launch":
        if args.commandsFile is not None:
            # Commands are listed in a file.
            jobname = args.commandsFile.name
            commands = smartdispatch.get_commands_from_file(args.commandsFile)
        else:
            # Command that needs to be parsed and unfolded.
            command = " ".join(args.commandAndOptions)
            jobname = smartdispatch.generate_name_from_command(command, max_length=235)
            commands = smartdispatch.unfold_command(command)

        commands = smartdispatch.replace_uid_tag(commands)
        nb_commands = len(commands)  # For print at the end

        path_job_logs, path_job_commands = create_job_folders(jobname)
    elif args.mode == "resume":
        path_job_logs, path_job_commands = get_job_folders(args.batch_uid)
    else:
        raise ValueError("Unknown subcommand!")

    # Pool of workers
    if args.pool is not None:
        command_manager = CommandManager(os.path.join(path_job_commands, "commands.txt"))

        # If resume mode, reset running jobs
        if args.mode == "launch":
            command_manager.set_commands_to_run(commands)
        else:
            command_manager.reset_running_commands()

        worker_command = 'smart_worker.py "{0}" "{1}"'.format(command_manager._commands_filename, path_job_logs)
        # Replace commands with `args.pool` workers
        commands = [worker_command] * args.pool

    # Add redirect for output and error logs
    for i, command in enumerate(commands):
        # Change directory before executing command
        commands[i] = 'cd "{cwd}"; '.format(cwd=os.getcwd()) + commands[i]
        # Log command's output and command's error
        log_filename = os.path.join(path_job_logs, smartdispatch.generate_name_from_command(command, max_length_arg=30))
        commands[i] += ' 1>> "{output_log}"'.format(output_log=log_filename + ".o")
        commands[i] += ' 2>> "{error_log}"'.format(error_log=log_filename + ".e")

    # TODO: use args.memPerNode instead of args.memPerNode
    queue = Queue(args.queueName, CLUSTER_NAME, args.walltime, args.coresPerNode, args.gpusPerNode, np.inf, args.modules)

    command_params = {'nb_cores_per_command': args.coresPerCommand,
                      'nb_gpus_per_command': args.gpusPerCommand,
                      'mem_per_command': None  # args.memPerCommand
                      }

    job_generator = job_generator_factory(queue, commands, command_params, CLUSTER_NAME)
    pbs_filenames = job_generator.write_pbs_files(path_job_commands)

    print "{nb_commands} command(s) will be executed in {nb_jobs} job(s).".format(nb_commands=nb_commands, nb_jobs=len(pbs_filenames))
    print "Batch UID: {batch_uid}".format(batch_uid=jobname)
    # Launch the jobs with QSUB
    if not args.doNotLaunch:
        job_ids = "Job id's: "
        for pbs_filename in pbs_filenames:
            qsub_output = check_output('{launcher} {pbs_filename}'.format(launcher=LAUNCHER if args.launcher is None else args.launcher, pbs_filename=pbs_filename), shell=True)
            job_ids += "{} ".format(qsub_output.rstrip())
        print job_ids

    print "\nLogs, command, and job id's related to this batch will be in: {smartdispatch_folder}/{{Batch UID}}".format(smartdispatch_folder=LOGS_FOLDERNAME)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--queueName', required=True, help='Queue used (ex: qwork@mp2, qfat256@mp2, qfat512@mp2)')
    parser.add_argument('-t', '--walltime', required=False, help='Set the estimated running time of your jobs using the DD:HH:MM:SS format. Note that they will be killed when this time limit is reached.')
    parser.add_argument('-L', '--launcher', choices=['qsub', 'msub'], required=False, help='Which launcher to use. Default: qsub')
    parser.add_argument('-C', '--coresPerNode', type=int, required=False, help='How many cores there are per node.')
    parser.add_argument('-G', '--gpusPerNode', type=int, required=False, help='How many gpus there are per node.')
    #parser.add_argument('-M', '--memPerNode', type=int, required=False, help='How much memory there are per node (in Gb).')

    parser.add_argument('-c', '--coresPerCommand', type=int, required=False, help='How many cores a command needs.', default=1)
    parser.add_argument('-g', '--gpusPerCommand', type=int, required=False, help='How many gpus a command needs.', default=1)
    #parser.add_argument('-m', '--memPerCommand', type=float, required=False, help='How much memory a command needs (in Gb).')
    parser.add_argument('-f', '--commandsFile', type=file, required=False, help='File containing commands to launch. Each command must be on a seperate line. (Replaces commandAndOptions)')

    parser.add_argument('-l', '--modules', type=str, required=False, help='List of additional modules to load.', nargs='+')
    parser.add_argument('-x', '--doNotLaunch', action='store_true', help='Creates the QSUB files without launching them.')

    parser.add_argument('-p', '--pool', type=int, help="Number of workers that will be consuming commands.")
    subparsers = parser.add_subparsers(dest="mode")

    launch_parser = subparsers.add_parser('launch', help="Launch jobs.")
    launch_parser.add_argument("commandAndOptions", help="Options for the commands.", nargs=argparse.REMAINDER)

    resume_parser = subparsers.add_parser('resume', help="Resume jobs from batch UID.")
    resume_parser.add_argument("batch_uid", help="Batch UID of the jobs to resume.")

    args = parser.parse_args()

    # Check for invalid arguments in
    if args.mode == "launch":
        if args.commandsFile is None and len(args.commandAndOptions) < 1:
            parser.error("You need to specify a command to launch.")
        if args.queueName not in AVAILABLE_QUEUES and ((args.coresPerNode is None and args.gpusPerNode is None) or args.walltime is None):
            parser.error("Unknown queue, --coresPerNode/--gpusPerNode and --walltime must be set.")
    else:
        if args.pool is None:
            resume_parser.error("The resume feature only works with the --pool argument.")

    return args


def _gen_job_paths(jobname):
    path_smartdispatch_logs = os.path.join(os.getcwd(), LOGS_FOLDERNAME)
    path_job = os.path.join(path_smartdispatch_logs, jobname)
    path_job_logs = os.path.join(path_job, 'logs')
    path_job_commands = os.path.join(path_job, 'commands')

    return path_job_logs, path_job_commands


def get_job_folders(jobname):
    path_job_logs, path_job_commands = _gen_job_paths(jobname)

    if not os.path.exists(path_job_commands):
        raise LookupError("Batch UID ({0}) does not exist! Cannot resume.".format(jobname))

    if not os.path.exists(path_job_logs):
        os.makedirs(path_job_logs)

    return path_job_logs, path_job_commands


def create_job_folders(jobname):
    """Creates the folders where the logs, commands and QSUB files will be saved."""
    path_job_logs, path_job_commands = _gen_job_paths(jobname)

    if not os.path.exists(path_job_commands):
        os.makedirs(path_job_commands)

    if not os.path.exists(path_job_logs):
        os.makedirs(path_job_logs)

    return path_job_logs, path_job_commands


if __name__ == "__main__":
    main()
