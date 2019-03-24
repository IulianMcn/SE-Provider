import re

class StringSplitRuleProvider:

    @staticmethod
    def alpha_numeric_splitting(str_to_split):
        return  re.findall(r'\w+',str_to_split)