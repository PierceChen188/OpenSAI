import json
import threading
import weakref
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

    # ===== NDJSON 分帧消息收发(替代裸 recv(65535)+is_json,消除 TCP 粘包/分段丢消息) =====
    # 每条控制面消息 = 一行 JSON + '\n';json.dumps 默认不产生换行,故换行可作可靠帧边界。
    _send_locks = weakref.WeakKeyDictionary()
    _send_locks_guard = threading.Lock()

    @classmethod
    def _send_lock(cls, sock):
        # 同一 socket 可能被多个线程并发写(如派发线程 + 该连接的处理线程),
        # 用每 socket 一把锁保证一帧 sendall 原子、不与其它帧交错。
        with cls._send_locks_guard:
            lk = cls._send_locks.get(sock)
            if lk is None:
                lk = threading.Lock()
                cls._send_locks[sock] = lk
            return lk

    @classmethod
    def send_msg(cls, sock, obj) -> None:
        """把一个对象作为一帧 NDJSON 发出(每 socket 原子)。obj 为 dict/可 json 序列化对象。"""
        line = (json.dumps(obj) + '\n').encode('utf-8')
        with cls._send_lock(sock):
            sock.sendall(line)

    @classmethod
    def send_line(cls, sock, text: str) -> None:
        """把**已序列化**的 JSON 字符串作为一帧发出(每 socket 原子)。"""
        line = (text.rstrip('\n') + '\n').encode('utf-8')
        with cls._send_lock(sock):
            sock.sendall(line)

    @classmethod
    def recv_msg(cls, sock, buf: bytearray):
        """用调用方持有的 bytearray 缓冲,从 sock 读出**恰好一条** NDJSON 消息并返回解析后的对象;
        连接关闭/EOF 返回 None。空行跳过。粘包/分段都由缓冲正确处理。"""
        while True:
            nl = buf.find(b'\n')
            if nl >= 0:
                line = bytes(buf[:nl])
                del buf[:nl + 1]
                if not line.strip():
                    continue
                return json.loads(line.decode('utf-8'))
            chunk = sock.recv(65536)
            if not chunk:
                return None
            buf.extend(chunk)
