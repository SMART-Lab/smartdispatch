# -*- coding: utf-8 -*-
import smartdispatch.utils as utils

from nose.tools import assert_equal
from numpy.testing import assert_array_equal


def test_chunks():
    sequence = range(10)

    for n in range(1, 11):
        expected = []
        for start, end in zip(range(0, len(sequence), n), range(n, len(sequence)+n, n)):
            expected.append(sequence[start:end])

        assert_array_equal(list(utils.chunks(sequence, n)), expected, "n:{0}".format(n))


def test_generate_uid():
    for obj in ["string", 42, 21., [1, "2", 3.], {'k': 'v'}]:
        assert_equal(utils.generate_uid(obj), utils.generate_uid(obj))


def test_slugify():
    testing_arguments = [("command", "command"),
                         ("/path/to/arg2/", "pathtoarg2"),
                         ("!\"/$%?&*()[]~{<>'.#|\\", ""),
                         (u"éèàëöüùò±@£¢¤¬¦²³¼½¾", "eeaeouuo23141234"),  # ¼ => 1/4 => 14
                         ("arg with space", "arg_with_space")]

    for arg, expected in testing_arguments:
        assert_equal(utils.slugify(arg), expected)
