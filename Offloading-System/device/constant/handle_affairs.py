from enum import Enum


class HandleAffairs(Enum):
    QUERY = "QUERY"  # 查询
    MEAN = "MEAN"  # 取平均
    SUM = "SUM"  # 求和
    WORD_COUNT = "WORD_COUNT"  # 词频统计
    IMAGES_PROCESSING = "IMAGES_PROCESSING"  # 图片处理
    LICENSE_PLATE = "LICENSE_PLATE"  # 车牌识别
