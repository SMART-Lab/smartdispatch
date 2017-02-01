import re
from collections import OrderedDict

regex_walltime = re.compile("(\d+:){1,4}")
regex_resource_nodes = re.compile("[a-zA-Z0-9]+(:ppn=\d+)?(:gpus=\d+)?(:[a-zA-Z0-9]+)*")
regex_resource_pmem = re.compile("[0-9]+(b|kb|mb|gb|tb)?")


class PBS(object):
    """ Offers functionalities to manage a PBS file.

    For more information about the PBS file format see:
    `http://docs.adaptivecomputing.com/suite/8-0/basic/help.htm#topics/torque/2-jobs/requestingRes.htm?TocPath=TORQUE Resource Manager|Submitting and managing jobs|Job submission|_____3`

    Parameters
    ----------
    queue_name : str
        name of the queue on which commands will be executed
    walltime : str
        maximum time allocated to execute every commands (DD:HH:MM:SS)
    """
    def __init__(self, queue_name, walltime):
        if queue_name is None or len(queue_name) == 0:
            raise ValueError("Queue's name must be provided.")

        self.queue_name = queue_name
        self.modules = []
        self.prolog = []
        self.commands = []
        self.epilog = []

        self.resources = OrderedDict()
        self.add_resources(walltime=walltime)

        self.options = OrderedDict()
        self.add_options(q=queue_name)

        # Declares that all environment variables in the qsub command's environment are to be exported to the batch job.
        self.add_options(V="")

    def add_options(self, **options):
        """ Adds options to this PBS file.

        Parameters
        ----------
        **options : dict
            each key is the name of a PBS option (see `Options`)

        Options
        -------
        *A* : account_string
            Defines the account string associated with the job.
        *N* : name (up to 64 characters)
            Declares a name for the job. It must consist of printable,
            non white space characters with the first character alphabetic.
        """
        for option_name, option_value in options.items():
            # If known option, validate it.
            if option_name.strip('-') == 'N':
                if len(option_name) > 64:
                    raise ValueError("Maximum number of characters for the name is: 64")

            self.options["-" + option_name] = option_value

    def add_resources(self, **resources):
        """ Adds resources to this PBS file.

        Parameters
        ----------
        **resources : dict
            each key is the name of a PBS resource (see `Resources`)

        Resources
        ---------
        *nodes* : nodes={<node_count>|<hostname>}[:ppn=<ppn>][:gpus=<gpu>][:<property>[:<property>]...]
            Specifies how many and what type of nodes to use
            **nodes={<node_count>|<hostname>}**: type of nodes
            **ppn=#**: Number of process per node requested for this job
            **gpus=#**: Number of process per node requested for this job
            **property**: A string specifying a node's feature
        *pmem*: pmem=[0-9]+(b|kb|mb|gb|tb)
            Specifies the maximum amount of physical memory used by any single process of the job.
        """
        for resource_name, resource_value in resources.items():
            # If known ressource, validate it.
            if resource_name == 'nodes':
                if re.match(regex_resource_nodes, str(resource_value)) is None:
                    raise ValueError("Unknown format for PBS resource: nodes")
            elif resource_name == 'pmem':
                if re.match(regex_resource_pmem, str(resource_value)) is None:
                    raise ValueError("Unknown format for PBS resource: pmem")
            elif resource_name == 'walltime':
                if re.match(regex_walltime, str(resource_value)) is None:
                    raise ValueError("Unknown format for PBS resource: walltime (dd:hh:mm:ss)")

            self.resources[resource_name] = resource_value

    def add_modules_to_load(self, *modules):
        """ Adds modules to load prior to execute the job on a node.

        Parameters
        ----------
        *modules : list of str
            each string represents the name of the module to load
        """
        self.modules += modules

    def add_to_prolog(self, *code):
        """ Adds the code to be executed before the commands.

        Parameters
        ----------
        *code : list of str
            Each string holds the code to be executed before the commands
        """
        self.prolog += code

    def add_commands(self, *commands):
        """ Sets commands to execute on a node.

        Parameters
        ----------
        *commands : list of str
            each string represents a command that is part of this job
        """
        self.commands += commands

    def add_to_epilog(self, *code):
        """ Adds the code to be executed after the commands.

        Parameters
        ----------
        *code : list of str
            Each string holds the code to be executed after the commands
        """
        self.epilog += code

    def save(self, filename):
        """ Saves this PBS job to a file.

        Parameters
        ----------
        filename : str
            specified where to save this PBS file
        """
        with open(filename, 'w') as pbs_file:
            pbs_file.write(str(self))

    def __str__(self):
        pbs = []
        pbs += ["#!/bin/bash"]

        for option_name, option_value in self.options.items():
            if option_value == "":
                pbs += ["#PBS {0}".format(option_name)]
            else:
                pbs += ["#PBS {0} {1}".format(option_name, option_value)]

        for resource_name, resource_value in self.resources.items():
            pbs += ["#PBS -l {0}={1}".format(resource_name, resource_value)]

        pbs += ["\n# Modules #"]
        for module in self.modules:
            pbs += ["module load " + module]

        pbs += ["\n# Prolog #"]
        pbs += self.prolog

        pbs += ["\n# Commands #"]
        pbs += ["{command}".format(command=command) for command in self.commands]

        pbs += ["\n# Epilog #"]
        pbs += self.epilog

        return "\n".join(pbs)
