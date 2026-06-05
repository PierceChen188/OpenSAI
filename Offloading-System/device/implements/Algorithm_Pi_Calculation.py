__all__ = ['Algorithm_Pi_Calculation']

import random


class Algorithm_Pi_Calculation:
    def __init__(self, param_dict, data_dict):
        self.num_samples = param_dict

    def run(self):
        inside_circle = 0
        total_num = self.num_samples['count_num'] if 'count_num' in self.num_samples else 10000
        for _ in range(total_num):
            x = random.uniform(-1, 1)
            y = random.uniform(-1, 1)
            if x ** 2 + y ** 2 <= 1:
                inside_circle += 1
        # print("task pi is:", self.num_samples['count_num'], 4 * inside_circle / self.num_samples['count_num'])
        return 4 * inside_circle / total_num
