__all__ = ['Algorithm_Random']

import random

class Algorithm_Random:
    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
        randNum = random.randint(1, 3)
        result = "TaskRandom finished!  {}".format(randNum)
        return result
