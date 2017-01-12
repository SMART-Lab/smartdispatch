# -*- coding: utf-8 -*-
import unittest

from smartdispatch import utils

from nose.tools import assert_equal, assert_true
from numpy.testing import assert_array_equal


class PrintBoxedTests(unittest.TestCase):

    def setUp(self):
        self.empty = ''
        self.text = "This is weird test for a visual thing.\nWell maybe it's fine to test it's working."

    def test_print_boxed(self):
        utils.print_boxed(self.text)

    def test_print_boxed_empty(self):
        utils.print_boxed(self.empty)


def test_chunks():
    sequence = range(10)

    for n in range(1, 11):
        expected = []
        for start, end in zip(range(0, len(sequence), n), range(n, len(sequence) + n, n)):
            expected.append(sequence[start:end])

        assert_array_equal(list(utils.chunks(sequence, n)), expected, "n:{0}".format(n))


def test_generate_uid_from_string():
    assert_equal(utils.generate_uid_from_string("same text"), utils.generate_uid_from_string("same text"))
    assert_true(utils.generate_uid_from_string("same text") != utils.generate_uid_from_string("sametext"))

def test_jobname_generator():
    assertNotEqual(utils.jobname_generator("a-string-which-has-a-longer-length-nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn-than-64", 8798777), 63)
    assertNotEqual(utils.jobname_generator("abcde",3), 7)
    assertNotEqual(utils.jobname_generator(("",21), 3)

def test_slugify():
    testing_arguments = [("command", "command"),
                         ("/path/to/arg2/", "pathtoarg2"),
                         ("!\"/$%?&*()[]~{<>'.#|\\", ""),
                         ("éèàëöüùò±@£¢¤¬¦²³¼½¾", "eeaeouuo23141234"),  # ¼ => 1/4 => 14
                         ("arg with space", "arg_with_space")]

    for arg, expected in testing_arguments:
        assert_equal(utils.slugify(arg), expected)
