__all__ = ['Algorithm_Sleep_1']

import time
import random

class Algorithm_Sleep_1:
    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
        sec = self.param['sleep_time'] if 'sleep_time' in self.param else 1
        time.sleep(sec)
        result = "Sleep task finished!  {}".format(sec)
        return result
