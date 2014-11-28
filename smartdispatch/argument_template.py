import re
from collections import OrderedDict


def build_argument_templates_dictionnary():
    # Order matter, if some regex is more greedy than another, the it should go after
    argument_templates = OrderedDict()
    argument_templates[RangeArgumentTemplate.__name__] = RangeArgumentTemplate()
    argument_templates[ListArgumentTemplate.__name__] = ListArgumentTemplate()
    return argument_templates


class ArgumentTemplate(object):
    def __init__(self):
        self.regex = ""

    def unfold(self, match):
        raise NotImplementedError("Subclass must implement method `unfold(self, match)`!")


class ListArgumentTemplate(ArgumentTemplate):
    def __init__(self):
        self.regex = "\[[^]]*\]"

    def unfold(self, match):
        return match[1:-1].split(' ')


class RangeArgumentTemplate(ArgumentTemplate):
    def __init__(self):
        self.regex = "\[(\d+):(\d+)(?::(\d+))?\]"

    def unfold(self, match):
        groups = re.search(self.regex, match).groups()
        start = int(groups[0])
        end = int(groups[1])
        step = 1 if groups[2] is None else int(groups[2])
        return map(str, range(start, end, step))


argument_templates = build_argument_templates_dictionnary()
