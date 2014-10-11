from nose.tools import assert_true, assert_equal, assert_raises
from numpy.testing import assert_array_equal

from smartdispatch.cluster import Queue, MammouthQueue, GuilliminQueue
from smartdispatch.cluster import queue_factory

import copy
import unittest
import tempfile
import shutil
import os


class TestQueue(unittest.TestCase):
    def setUp(self):
        self.testing_dir = tempfile.mkdtemp()

        self.name = "qtest@mp2"
        self.walltime = "10:00"
        self.cores = 42
        self.gpus = 0
        self.modules = ["cuda", "python"]

        self.queue = Queue(self.name, self.walltime, self.cores, self.gpus, self.modules)

    def tearDown(self):
        shutil.rmtree(self.testing_dir)

    def test_generate_pbs(self):
        commands = ["echo 1", "echo 2", "echo 3", "echo 4"]

        # Should needs one PBS file
        pbs_list = self.queue.generate_pbs(commands)
        assert_equal(len(pbs_list), 1)
        assert_equal(pbs_list[0].commands, commands)

        # Should needs two PBS file
        nb_cores_per_command = self.cores // 2
        pbs_list = self.queue.generate_pbs(commands, nb_cores_per_command)
        assert_equal(len(pbs_list), 2)
        assert_equal(pbs_list[0].commands, commands[:2])
        assert_equal(pbs_list[1].commands, commands[2:])

        # Should needs four PBS file
        pbs_list = self.queue.generate_pbs(commands, nb_cores_per_command=self.cores)
        assert_equal(len(pbs_list), 4)
        assert_equal([pbs.commands[0] for pbs in pbs_list], commands)

        # If queue has gpus it should be specified in PBS resource `nodes`
        assert_true('gpus' not in pbs_list[0].resources['nodes'])

        queue = copy.copy(self.queue)
        queue.gpus = 2
        pbs_list = queue.generate_pbs(commands)
        assert_true('gpus' in pbs_list[0].resources['nodes'])

        # Check if needed modules for this queue are included in the PBS file
        assert_equal(pbs_list[0].modules, self.modules)


class TestGuilliminQueue(unittest.TestCase):
    def setUp(self):
        self.testing_dir = tempfile.mkdtemp()

        self.name = "qtest@mp2"
        self.walltime = "10:00"
        self.cores = 42
        self.gpus = 0
        self.modules = ["cuda", "python"]

        self.queue = GuilliminQueue(self.name, self.walltime, self.cores, self.gpus, self.modules)

    def tearDown(self):
        shutil.rmtree(self.testing_dir)

    def test_generate_pbs(self):
        commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        pbs = self.queue.generate_pbs(commands)[0]
        assert_true("-A" in pbs.options)


def test_queue_factory():
    queue = queue_factory(name="qtest@mp2")
    assert_true(isinstance(queue, MammouthQueue))

    queue = queue_factory(name="k20")
    assert_true(isinstance(queue, GuilliminQueue))

    assert_raises(ValueError, queue_factory, name="smart@lab")
    assert_raises(ValueError, queue_factory, name="smart@lab")
    assert_raises(ValueError, queue_factory, name="smart@lab", walltime="10:00")
    assert_raises(ValueError, queue_factory, name="smart@lab", cores=12)

    queue = queue_factory(name="smart@lab", walltime="10:00", cores=12)
    assert_true(isinstance(queue, Queue))
