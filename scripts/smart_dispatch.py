#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
import time as t
import numpy as np
from subprocess import check_output
from textwrap import dedent

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
            jobname = os.path.basename(args.commandsFile.name)
            commands = smartdispatch.get_commands_from_file(args.commandsFile)
        else:
            # Command that needs to be parsed and unfolded.
            command = " ".join(args.commandAndOptions)
            jobname = smartdispatch.generate_name_from_command(command, max_length=235)
            commands = smartdispatch.unfold_command(command)

        commands = smartdispatch.replace_uid_tag(commands)
        nb_commands = len(commands)  # For print at the end

        path_job, path_job_logs, path_job_commands = create_job_folders(jobname)
    elif args.mode == "resume":
        jobname = args.batch_uid
        path_job, path_job_logs, path_job_commands = get_job_folders(args.batch_uid)
    else:
        raise ValueError("Unknown subcommand!")

    command_manager = CommandManager(os.path.join(path_job_commands, "commands.txt"))

    # If resume mode, reset running jobs
    if args.mode == "launch":
        command_manager.set_commands_to_run(commands)
    elif args.mode == "resume":
        # Verifying if there are failed commands
        failed_commands = command_manager.get_failed_commands()
        if len(failed_commands) > 0:
            FAILED_COMMAND_MESSAGE = dedent("""\
            {nb_failed} command(s) are in a failed state. They won't be resumed.
            Failed commands:
            {failed_commands}
            The actual errors can be found in the log folder under:
            {failed_commands_err_file}""")
            utils.print_boxed(FAILED_COMMAND_MESSAGE.format(
                nb_failed=len(failed_commands),
                failed_commands=''.join(failed_commands),
                failed_commands_err_file='\n'.join([utils.generate_uid_from_string(c[:-1]) + '.err' for c in failed_commands])
            ))

            if not utils.yes_no_prompt("Do you want to continue?", 'n'):
                exit()

        if not args.only_pending:
            command_manager.reset_running_commands()

        nb_commands = command_manager.get_nb_commands_to_run()

    # If no pool size is specified the number of commands is taken
    if args.pool is None:
        args.pool = command_manager.get_nb_commands_to_run()

    # Generating all the worker commands
    COMMAND_STRING = 'cd "{cwd}"; smart_worker.py "{commands_file}" "{log_folder}" '\
                     '1>> "{log_folder}/worker/$PBS_JOBID\"\"_worker_{{ID}}.o" '\
                     '2>> "{log_folder}/worker/$PBS_JOBID\"\"_worker_{{ID}}.e" '
    COMMAND_STRING = COMMAND_STRING.format(cwd=os.getcwd(), commands_file=command_manager._commands_filename, log_folder=path_job_logs)
    commands = [COMMAND_STRING.format(ID=i) for i in range(args.pool)]

    # TODO: use args.memPerNode instead of args.memPerNode
    queue = Queue(args.queueName, CLUSTER_NAME, args.walltime, args.coresPerNode, args.gpusPerNode, np.inf, args.modules)

    command_params = {'nb_cores_per_command': args.coresPerCommand,
                      'nb_gpus_per_command': args.gpusPerCommand,
                      'mem_per_command': None  # args.memPerCommand
                      }

    job_generator = job_generator_factory(queue, commands, command_params, CLUSTER_NAME, path_job)
    pbs_filenames = job_generator.write_pbs_files(path_job_commands)

    # Launch the jobs
    print "## {nb_commands} command(s) will be executed in {nb_jobs} job(s) ##".format(nb_commands=nb_commands, nb_jobs=len(pbs_filenames))
    print "Batch UID:\n{batch_uid}".format(batch_uid=jobname)
    if not args.doNotLaunch:
        jobs_id = []
        for pbs_filename in pbs_filenames:
            qsub_output = check_output('{launcher} {pbs_filename}'.format(launcher=LAUNCHER if args.launcher is None else args.launcher, pbs_filename=pbs_filename), shell=True)
            jobs_id += [qsub_output.strip()]

        with utils.open_with_lock(os.path.join(path_job, "jobs_id.txt"), 'a') as jobs_id_file:
            jobs_id_file.writelines(t.strftime("## %Y-%m-%d %H:%M:%S ##\n"))
            jobs_id_file.writelines("\n".join(jobs_id) + "\n")
        print "\nJobs id:\n{jobs_id}".format(jobs_id=" ".join(jobs_id))
    print "\nLogs, command, and jobs id related to this batch will be in:\n {smartdispatch_folder}".format(smartdispatch_folder=path_job)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--queueName', required=True, help='Queue used (ex: qwork@mp2, qfat256@mp2, qfat512@mp2)')
    parser.add_argument('-t', '--walltime', required=False, help='Set the estimated running time of your jobs using the DD:HH:MM:SS format. Note that they will be killed when this time limit is reached.')
    parser.add_argument('-L', '--launcher', choices=['qsub', 'msub'], required=False, help='Which launcher to use. Default: qsub')
    parser.add_argument('-C', '--coresPerNode', type=int, required=False, help='How many cores there are per node.')
    parser.add_argument('-G', '--gpusPerNode', type=int, required=False, help='How many gpus there are per node.')
    # parser.add_argument('-M', '--memPerNode', type=int, required=False, help='How much memory there are per node (in Gb).')

    parser.add_argument('-c', '--coresPerCommand', type=int, required=False, help='How many cores a command needs.', default=1)
    parser.add_argument('-g', '--gpusPerCommand', type=int, required=False, help='How many gpus a command needs.', default=1)
    # parser.add_argument('-m', '--memPerCommand', type=float, required=False, help='How much memory a command needs (in Gb).')
    parser.add_argument('-f', '--commandsFile', type=file, required=False, help='File containing commands to launch. Each command must be on a seperate line. (Replaces commandAndOptions)')

    parser.add_argument('-l', '--modules', type=str, required=False, help='List of additional modules to load.', nargs='+')
    parser.add_argument('-x', '--doNotLaunch', action='store_true', help='Creates the QSUB files without launching them.')

    parser.add_argument('-p', '--pool', type=int, help="Number of workers that will be consuming commands. Default: Nb commands")
    subparsers = parser.add_subparsers(dest="mode")

    launch_parser = subparsers.add_parser('launch', help="Launch jobs.")
    launch_parser.add_argument("commandAndOptions", help="Options for the commands.", nargs=argparse.REMAINDER)

    resume_parser = subparsers.add_parser('resume', help="Resume jobs from batch UID.")
    resume_parser.add_argument('--only_pending', action='store_true', help='Resume only pending commands.')
    resume_parser.add_argument("batch_uid", help="Batch UID of the jobs to resume.")

    args = parser.parse_args()

    # Check for invalid arguments in
    if args.mode == "launch":
        if args.commandsFile is None and len(args.commandAndOptions) < 1:
            parser.error("You need to specify a command to launch.")
        if args.queueName not in AVAILABLE_QUEUES and ((args.coresPerNode is None and args.gpusPerNode is None) or args.walltime is None):
            parser.error("Unknown queue, --coresPerNode/--gpusPerNode and --walltime must be set.")

    return args


def _gen_job_paths(jobname):
    path_smartdispatch_logs = os.path.join(os.getcwd(), LOGS_FOLDERNAME)
    path_job = os.path.join(path_smartdispatch_logs, jobname)
    path_job_logs = os.path.join(path_job, 'logs')
    path_job_commands = os.path.join(path_job, 'commands')

    return path_job, path_job_logs, path_job_commands


def get_job_folders(jobname):
    path_job, path_job_logs, path_job_commands = _gen_job_paths(jobname)

    if not os.path.exists(path_job_commands):
        raise LookupError("Batch UID ({0}) does not exist! Cannot resume.".format(jobname))

    if not os.path.exists(path_job_logs):
        os.makedirs(path_job_logs)
    if not os.path.exists(os.path.join(path_job_logs, "worker")):
        os.makedirs(os.path.join(path_job_logs, "worker"))
    if not os.path.exists(os.path.join(path_job_logs, "job")):
        os.makedirs(os.path.join(path_job_logs, "job"))

    return path_job, path_job_logs, path_job_commands


def create_job_folders(jobname):
    """Creates the folders where the logs, commands and QSUB files will be saved."""
    path_job, path_job_logs, path_job_commands = _gen_job_paths(jobname)

    if not os.path.exists(path_job_commands):
        os.makedirs(path_job_commands)

    if not os.path.exists(path_job_logs):
        os.makedirs(path_job_logs)
        os.makedirs(os.path.join(path_job_logs, "worker"))
        os.makedirs(os.path.join(path_job_logs, "job"))

    return path_job, path_job_logs, path_job_commands


if __name__ == "__main__":
    main()
