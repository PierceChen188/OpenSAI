import json
from typing import TypeVar


class NetworkUtils:
    T = TypeVar('T')

    @classmethod
    def is_json(cls, var: T) -> bool:
        """
        判断数据是否json类型
        :param var: 要判断的数据
        :return: True或者False
        """
        try:
            json.loads(var)
            return True
        except ValueError:
            return False

    @classmethod
    def is_dict(cls, var: T) -> bool:
        """
        判断数据是否字典(dict)数据类型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, dict)

    @classmethod
    def is_byte(cls, var: T) -> bool:
        """
        判断数据是否字节(byte)数据类型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, bytes)

    @classmethod
    def is_x(cls, var: T) -> bool:
        """
        判断数据是否填充字节类型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, bytes)

    @classmethod
    def is_c(cls, var: T) -> bool:
        """
        判断数据是否填充字符类型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, str)

    @classmethod
    def is_b(cls, var: T) -> bool:
        """
        判断数据是否有符号字符
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and -128 <= var <= 127

    @classmethod
    def is_B(cls, var: T) -> bool:
        """
        判断数据是否无符号字符
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and 0 <= var <= 255

    @classmethod
    def is_h(cls, var: T) -> bool:
        """
        判断数据是否有符号短整型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and -32768 <= var <= 32767

    @classmethod
    def is_H(cls, var: T) -> bool:
        """
        判断数据是否无符号短整型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and 0 <= var <= 65535

    @classmethod
    def is_i(cls, var: T) -> bool:
        """
        判断数据是否有符号整型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and -2147483648 <= var <= 2147483647

    @classmethod
    def is_I(cls, var: T) -> bool:
        """
        判断数据是否无符号整型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and 0 <= var <= 4294967295

    @classmethod
    def is_l(cls, var: T) -> bool:
        """
        判断数据是否有符号长整型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and -9223372036854775808 <= var <= 9223372036854775807

    @classmethod
    def is_L(cls, var: T) -> bool:
        """
        判断数据是否无符号长整型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and 0 <= var <= 18446744073709551615

    @classmethod
    def is_q(cls, var: T) -> bool:
        """
        判断数据是否有符号长长整型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and -9223372036854775808 <= var <= 9223372036854775807

    @classmethod
    def is_Q(cls, var: T) -> bool:
        """
        判断数据是否无符号长长整型
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, int) and 0 <= var <= 18446744073709551615

    @classmethod
    def is_f(cls, var: T) -> bool:
        """
        判断数据是否单精度浮点数
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, float)

    @classmethod
    def is_d(cls, var: T) -> bool:
        """
        判断数据是否双精度浮点数
        :param var: 要判断的数据
        :return: True或者False
        """
        return isinstance(var, float)
