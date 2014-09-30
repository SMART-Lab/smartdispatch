#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
import math
from subprocess import check_output

from smartdispatch import utils
from smartdispatch.command_manager import CommandManager

import smartdispatch


AVAILABLE_QUEUES = {
    # Mammouth Parallel
    'qtest@mp2': {'coresPerNode': 24, 'maxWalltime': '00:01:00:00'},
    'qwork@mp2': {'coresPerNode': 24, 'maxWalltime': '05:00:00:00'},
    'qfbb@mp2': {'coresPerNode': 288, 'maxWalltime': '05:00:00:00'},
    'qfat256@mp2': {'coresPerNode': 48, 'maxWalltime': '05:00:00:00'},
    'qfat512@mp2': {'coresPerNode': 48, 'maxWalltime': '02:00:00:00'},

    # Mammouth Série
    'qtest@ms': {'coresPerNode': 8, 'maxWalltime': '00:01:00:00'},
    'qwork@ms': {'coresPerNode': 8, 'maxWalltime': '05:00:00:00'},
    'qlong@ms': {'coresPerNode': 8, 'maxWalltime': '41:16:00:00'},

    # Mammouth GPU
    # 'qwork@brume' : {'coresPerNode' : 0, 'maxWalltime' : '05:00:00:00'} # coresPerNode is variable and not relevant for this queue
}


def main():
    args = parse_arguments()

    if args.commandsFile is not None:
        # Commands are listed in a file.
        jobname = args.commandsFile.name
        commands = smartdispatch.get_commands_from_file(args.commandsFile)
    else:
        # Commands that needs to be parsed and unfolded.
        arguments = map(smartdispatch.unfold_argument, args.commandAndOptions)
        jobname = smartdispatch.generate_name_from_arguments(arguments)
        commands = smartdispatch.get_commands_from_arguments(arguments)

    #Check for {UID} tag, if found replace with the command's uid.
    commands = smartdispatch.replace_uid_tag(commands)

    job_directory, qsub_directory = create_job_folders(jobname)

    # Pool of workers
    if args.pool is not None:
        command_manager = CommandManager(os.path.join(qsub_directory, "commands.txt"))
        command_manager.set_commands_to_run(commands)

        worker_command = 'smart_worker.py "{0}" "{1}"'.format(command_manager._commands_filename, job_directory)
        # Replace commands with `args.pool` workers
        commands = [worker_command] * args.pool

    # Distribute equally the jobs among the QSUB files and generate those files
    nb_commands = len(commands)
    nb_jobs = int(math.ceil(nb_commands / float(args.nbCommandsPerNode)))
    nb_commands_per_file = int(math.ceil(nb_commands / float(nb_jobs)))

    qsub_filenames = []
    for i, commands_per_file in enumerate(utils.chunks(commands, n=nb_commands_per_file)):
        qsub_filename = os.path.join(qsub_directory, 'jobCommands_' + str(i) + '.sh')
        write_qsub_file(commands_per_file, qsub_filename, job_directory, args.queueName, args.walltime, os.getcwd(), args.cuda)
        qsub_filenames.append(qsub_filename)

    # Launch the jobs with QSUB
    if not args.doNotLaunch:
        for qsub_filename in qsub_filenames:
            qsub_output = check_output('qsub ' + qsub_filename, shell=True)
            print qsub_output,


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--queueName', required=True, help='Queue used (ex: qwork@mp2, qfat256@mp2, qfat512@mp2)')
    parser.add_argument('-t', '--walltime', required=False, help='Set the estimated running time of your jobs using the DD:HH:MM:SS format. Note that they will be killed when this time limit is reached.')
    parser.add_argument('-n', '--nbCommandsPerNode', type=int, required=False, help='Set the number of commands per nodes.')
    parser.add_argument('-c', '--cuda', action='store_true', help='Load CUDA before executing your code.')
    parser.add_argument('-x', '--doNotLaunch', action='store_true', help='Creates the QSUB files without launching them.')
    parser.add_argument('-f', '--commandsFile', type=file, required=False, help='File containing commands to launch. Each command must be on a seperate line. (Replaces commandAndOptions)')
    parser.add_argument('--pool', type=int, help="Number of workers that will consume commands.")
    parser.add_argument("commandAndOptions", help="Options for the command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    # Check for invalid arguments
    if args.commandsFile is None and len(args.commandAndOptions) < 1:
        parser.error("You need to specify a command to launch.")
    if args.queueName not in AVAILABLE_QUEUES and (args.nbCommandsPerNode is None or args.walltime is None):
        parser.error("Unknown queue, --nbCommandsPerNode and --walltime must be set.")

    # Set queue defaults for non specified params
    if args.nbCommandsPerNode is None:
        args.nbCommandsPerNode = AVAILABLE_QUEUES[args.queueName]['coresPerNode']
    if args.walltime is None:
        args.walltime = AVAILABLE_QUEUES[args.queueName]['maxWalltime']

    return args


def create_job_folders(jobname):
    """Creates the folders where the logs, commands and QSUB files will be saved."""
    path_smartdispatch_logs = os.path.join(os.getcwd(), 'SMART_DISPATCH_LOGS')
    path_job = os.path.join(path_smartdispatch_logs, jobname)
    path_job_logs = os.path.join(path_job, 'logs')
    path_job_commands = os.path.join(path_job, 'commands')

    if not os.path.exists(path_job_commands):
        os.makedirs(path_job_commands)

    if not os.path.exists(path_job_logs):
        os.makedirs(path_job_logs)

    return path_job_logs, path_job_commands


def write_qsub_file(commands, pbs_filename, job_directory, queue, walltime, current_directory, use_cuda=False):
    with open(pbs_filename, 'w') as pbs_file:

        kwargs = {}
        if use_cuda:
            kwargs['module'] = ["cuda"]

        pbs = smartdispatch.generate_pbs(commands, queue, walltime, cwd=current_directory, logs_dir=job_directory, **kwargs)
        pbs_file.write(pbs)


if __name__ == "__main__":
    main()
