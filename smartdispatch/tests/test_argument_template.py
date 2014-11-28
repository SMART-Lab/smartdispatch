import re
from nose.tools import assert_true
from numpy.testing import assert_array_equal

from smartdispatch.argument_template import ListArgumentTemplate, RangeArgumentTemplate


def test_list_argument_template():
    # Test valid enumeration folded arguments
    arg = ListArgumentTemplate()
    folded_arguments = [("[]", [""]),
                        ("[1]", ["1"]),
                        ("[1 ]", ["1", ""]),
                        ("[1 2 3]", ["1", "2", "3"]),
                        ]

    for folded_argument, unfolded_arguments in folded_arguments:
        match = re.match(arg.regex, folded_argument)
        assert_true(match is not None)
        assert_array_equal(arg.unfold(match.group()), unfolded_arguments)

    # Test invalid enumeration folded arguments
    assert_true(re.match(arg.regex, "[1 2 3") is None)


def test_range_argument_template():
    arg = RangeArgumentTemplate()
    folded_arguments = [("[1:4]", ["1", "2", "3"]),
                        ("[1:4:2]", ["1", "3"]),
                        ]

    for folded_argument, unfolded_arguments in folded_arguments:
        match = re.match(arg.regex, folded_argument)
        assert_true(match is not None)
        assert_array_equal(arg.unfold(match.group()), unfolded_arguments)
