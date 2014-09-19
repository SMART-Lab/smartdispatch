# -*- coding: utf-8 -*-
import os
import time
import tempfile
import shutil

from subprocess import Popen, PIPE

import smartdispatch.utils as utils

from nose.tools import assert_equal, assert_true
from numpy.testing import assert_array_equal


def test_chunks():
    sequence = range(10)

    for n in range(1, 11):
        expected = []
        for start, end in zip(range(0, len(sequence), n), range(n, len(sequence)+n, n)):
            expected.append(sequence[start:end])

        assert_array_equal(list(utils.chunks(sequence, n)), expected, "n:{0}".format(n))


def test_generate_uid_from_string():
    assert_equal(utils.generate_uid_from_string("same text"), utils.generate_uid_from_string("same text"))
    assert_true(utils.generate_uid_from_string("same text") != utils.generate_uid_from_string("sametext"))


def test_slugify():
    testing_arguments = [("command", "command"),
                         ("/path/to/arg2/", "pathtoarg2"),
                         ("!\"/$%?&*()[]~{<>'.#|\\", ""),
                         (u"éèàëöüùò±@£¢¤¬¦²³¼½¾", "eeaeouuo23141234"),  # ¼ => 1/4 => 14
                         ("arg with space", "arg_with_space")]

    for arg, expected in testing_arguments:
        assert_equal(utils.slugify(arg), expected)


def test_open_and_lock():
    temp_dir = tempfile.mkdtemp()
    filename = os.path.join(temp_dir, "testing.lck")

    python_script = os.path.join(temp_dir, "test_lock.py")

    script = ["import logging",
              "from smartdispatch.utils import open_and_lock",
              "logging.root.setLevel(logging.INFO)",
              "with open_and_lock('{0}'): pass".format(filename)]

    open(os.path.join(temp_dir, "test_lock.py"), 'w').write("\n".join(script))

    command = "python " + python_script

    # Lock the commands file before running python command
    with utils.open_and_lock(filename, 'w'):
        process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        time.sleep(0.1)

    stdout, stderr = process.communicate()
    assert_equal(stdout, "")
    assert_true("write-lock" in stderr, msg="Forcing a race condition, try increasing sleeping time above.")

    shutil.rmtree(temp_dir)
