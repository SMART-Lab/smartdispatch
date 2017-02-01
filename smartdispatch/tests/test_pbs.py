from nose.tools import assert_true, assert_equal, assert_raises
from numpy.testing import assert_array_equal


from smartdispatch.pbs import PBS
import unittest
import tempfile
import shutil
import os


class TestPBS(unittest.TestCase):

    def setUp(self):
        self.testing_dir = tempfile.mkdtemp()
        self.queue_name = "qtest@cluster"
        self.walltime = "10:42"
        self.pbs = PBS(self.queue_name, self.walltime)

    def tearDown(self):
        shutil.rmtree(self.testing_dir)

    def test_constructor(self):
        assert_raises(ValueError, PBS, self.queue_name, None)
        assert_raises(ValueError, PBS, None, self.walltime)
        assert_raises(ValueError, PBS, "", self.walltime)
        assert_raises(ValueError, PBS, self.queue_name, "Wrong walltime format")

    def test_add_options(self):
        # Default options
        assert_equal(len(self.pbs.options), 2)
        assert_true('-V' in self.pbs.options.keys())
        assert_equal(self.pbs.options['-q'], self.queue_name)

        self.pbs.add_options(A="option1")
        assert_equal(self.pbs.options["-A"], "option1")
        self.pbs.add_options(A="option2", B="option3")
        assert_equal(self.pbs.options["-A"], "option2")
        assert_equal(self.pbs.options["-B"], "option3")

    def test_add_resources(self):
        assert_equal(len(self.pbs.resources), 1)
        assert_equal(self.pbs.resources["walltime"], self.walltime)

        self.pbs.add_resources(nodes="23:ppn=2")
        assert_equal(self.pbs.resources["nodes"], "23:ppn=2")

        self.pbs.add_resources(pmem=123, walltime="42:42")
        assert_equal(self.pbs.resources["pmem"], 123)
        assert_equal(self.pbs.resources["walltime"], "42:42")

    def test_add_modules_to_load(self):
        assert_equal(self.pbs.modules, [])

        modules = ["cuda", "gcc", "python/2.7"]
        self.pbs.add_modules_to_load(modules[0], modules[1])
        assert_array_equal(self.pbs.modules, modules[:2])

        self.pbs.add_modules_to_load(modules[-1])
        assert_array_equal(self.pbs.modules, modules)

    def test_add_commands(self):
        assert_equal(self.pbs.commands, [])

        commands = ["python my_script.py", "echo 1234", "git push origin master"]
        self.pbs.add_commands(commands[0])
        assert_array_equal(self.pbs.commands, commands[0])

        self.pbs.add_commands(*commands[1:])
        assert_array_equal(self.pbs.commands, commands)

    def test_str(self):
        # Create simple PBS file
        expected = """#!/bin/bash
#PBS -q qtest@mp2
#PBS -V
#PBS -l walltime=01:00:00

# Modules #

# Prolog #

# Commands #

# Epilog #"""
        pbs = PBS(queue_name="qtest@mp2", walltime="01:00:00")
        assert_equal(str(pbs), expected)

        # Create a more complex PBS file
        prolog = ['echo "This is prolog1"',
                  'echo "This is prolog2"']
        commands = ["cd $HOME; echo 1 2 3 1>> cmd1.o 2>> cmd1.e",
                    "cd $HOME; echo 3 2 1 1>> cmd1.o 2>> cmd1.e"]
        epilog = ['echo "This is epilog1"',
                  'echo "This is epilog2"']
        modules = ["CUDA_Toolkit/6.0", "python2.7"]

        expected = """\
#!/bin/bash
#PBS -q qtest@mp2
#PBS -V
#PBS -A xyz-123-ab
#PBS -l walltime=01:00:00
#PBS -l nodes=2:ppn=3:gpus=1

# Modules #
module load CUDA_Toolkit/6.0
module load python2.7

# Prolog #
{prolog1}
{prolog2}

# Commands #
{command1}
{command2}

# Epilog #
{epilog1}
{epilog2}""".format(
            prolog1=prolog[0], prolog2=prolog[1],
            command1=commands[0], command2=commands[1],
            epilog1=epilog[0], epilog2=epilog[1])

        pbs = PBS(queue_name="qtest@mp2", walltime="01:00:00")
        pbs.add_resources(nodes="2:ppn=3:gpus=1")
        pbs.add_options(A="xyz-123-ab")
        pbs.add_modules_to_load(*modules)
        pbs.add_to_prolog(*prolog)
        pbs.add_commands(*commands)
        pbs.add_to_epilog(*epilog)

        assert_equal(str(pbs), expected)

    def test_save(self):
        pbs_filename = os.path.join(self.testing_dir, "pbs.sh")
        self.pbs.save(pbs_filename)
        assert_equal(str(self.pbs), open(pbs_filename).read())
