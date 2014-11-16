import re


class FoldedArgument(object):
    def __init__(self):
        self.name = ""
        self.regex = ""

    def unfold(self, match):
        raise NotImplementedError("Subclass must implement this method!")


class EnumerationFoldedArgument(FoldedArgument):
    def __init__(self):
        self.name = "enumeration"
        self.regex = "\[[^]]*\]"

    def unfold(self, match):
        return match[1:-1].split(' ')


class RangeFoldedArgument(FoldedArgument):
    def __init__(self):
        self.name = "range"
        self.regex = "\[(\d+):(\d+)(?::(\d+))?\]"

    def unfold(self, match):
        groups = re.search(self.regex, match).groups()
        start = int(groups[0])
        end = int(groups[1])
        step = 1 if groups[2] is None else int(groups[2])
        return map(str, range(start, end, step))
