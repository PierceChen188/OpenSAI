class ComRedisKeyConstant:
    # 设备列表Key
    DEVICE_LIST = "DEVICE_LIST"

    # 单个设备信息KEY 前缀
    DEVICE_PREFIX = "DEVICE_PREFIX"

    # 单个设备信息KEY 前缀在线状态
    DEVICE_ONLINE_PREFIX = "DEVICE_ONLINE_PREFIX"

    # 单个设备信息KEY 前缀在线状态(netty)
    DEVICE_ONLINE_NETTY_PREFIX = "DEVICE_ONLINE_NETTY_PREFIX"

    # 单个设备信息KEY 前缀在线状态
    DEVICE_WEBSOCKET_PREFIX = "DEVICE_WEBSOCKET_PREFIX"

    # 单个设备信息KEY 前缀在线状态
    HETERO_TASK_DEVICE_ONLINE_PREFIX = "HETERO_TASK_DEVICE_ONLINE_PREFIX"

    # 单个设备信息KEY 前缀在线状态
    WORK_DEVICE_ONLINE_PREFIX = "WORK_DEVICE_ONLINE_PREFIX"

    # 单个设备ChannelId信息KEY 前缀在线状态
    WORK_DEVICE_CHANNEL_ID_PREFIX = "WORK_DEVICE_CHANNEL_ID_PREFIX"

    # 单个任务结果信息KEY 前缀在线状态
    RESULT_ONLINE_PREFIX = "RESULT_ONLINE_PREFIX"

    # 单个任务结果信息KEY 前缀在线状态(提供result结果给前端展示)
    RESULT_WEB_PREFIX = "RESULT_ONLINE_PREFIX"

    # 单个任务结果信息KEY 前缀在线状态(提供result结果给前端展示)
    RESULT_ONLINE_SET = "RESULT_ONLINE_SET"

    # 单个子任务进度信息KEY 前缀在线状态(提供子任务计算进度给前端展示)
    RESULT_WEB_PROGRESS = "RESULT_WEB_PROGRESS"

    # 单个子任务进度信息KEY 前缀在线状态(提供result结果给前端展示)
    RESULT_PROGRESS_SET = "RESULT_PROGRESS_SET"

    # 单个子任务进度结果缓存信息KEY (提供给LBFO进行二次调度)
    RESULT_LBFO_PROGRESS = "RESULT_LBFO_PROGRESS"

    # 在线设备列表Set集合Key 设备编号集合
    HETERO_TASK_DEVICE_ONLINE_SET = "HETERO_TASK_DEVICE_ONLINE_SET"

    # 在线设备列表Set集合Key 设备编号集合
    DEVICE_ONLINE_SET = "DEVICE_ONLINE_SET"

    # 在线设备列表Set集合Key 设备编号集合(netty)
    DEVICE_ONLINE_NETTY_SET = "DEVICE_ONLINE_NETTY_SET"

    # 在线空闲设备Set集合Key
    DEVICE_ONLINE_IDLE_SET = "DEVICE_ONLINE_IDLE_SET"

    # 在线设备列表Set集合Key 设备编号集合(后端调度使用)
    DEVICE_WEBSOCKET_SET = "DEVICE_WEBSOCKET_SET"

    # 在线设备列表Set集合Key 设备编号集合(后端netty调度使用)
    DEVICE_NETTY_SET = "DEVICE_NETTY_SET"

    # 在线设备性能列表Set集合Key 设备编号集合
    DEVICE_ONLINE_PERFORMANCE_SET = "DEVICE_ONLINE_PERFORMANCE_SET"

    # 在线设备空闲内存列表Set集合Key 设备编号集合
    DEVICE_ONLINE_MEMORY_SET = "DEVICE_ONLINE_MEMORY_SET"

    # 在线设备性能列表Set集合Key 设备编号集合(netty)
    DEVICE_ONLINE_PERFORMANCE_NETTY_SET = "DEVICE_ONLINE_PERFORMANCE_NETTY_SET"

    # 在线设备性能列表Set集合Key 设备编号集合
    DEVICE_WEBSOCKET_PERFORMANCE_SET = "DEVICE_WEBSOCKET_PERFORMANCE_SET"

    # 在线设备距离列表Set集合Key 设备编号集合
    DEVICE_ONLINE_DELAY_SET = "DEVICE_ONLINE_DELAY_SET"

    # 在线设备距离列表Set集合Key 设备编号集合(netty)
    DEVICE_ONLINE_DISTANCE_NETTY_SET = "DEVICE_ONLINE_DISTANCE_NETTY_SET"

    # 在线设备距离列表Set集合Key 设备编号集合
    DEVICE_WEBSOCKET_DISTANCE_SET = "DEVICE_WEBSOCKET_DISTANCE_SET"

    # 在线设备列表Set集合Key 设备编号集合
    WORK_DEVICE_ONLINE_SET = "WORK_DEVICE_ONLINE_SET"

    # 在线设备channelId列表Set集合Key 设备编号集合
    WORK_DEVICE_CHANNEL_ID_SET = "WORK_DEVICE_CHANNEL_ID_SET"

    # 在线异构任务列表Set集合Key 任务编号集合
    HETERO_TASK_ONLINE_SET = "HETERO_TASK_ONLINE_SET"

    # 在线任务列表Set集合Key 任务编号集合
    TASK_ONLINE_SET = "TASK_ONLINE_SET"

    # 阻塞任务列表Set集合Key 任务编号集合
    BLOCK_TASK_ONLINE_SET = "BLOCK_TASK_ONLINE_SET"

    # 在线任务列表Set集合Key 任务编号集合
    RANK_TASK_SET = "RANK_TASK_SET"

    # 在线工作任务列表Set集合Key 工作任务编号集合
    WORK_TASK_ONLINE_SET = "WORK_TASK_ONLINE_SET"

    # 未工作任务列表Set集合Key 工作任务编号集合
    UN_WORK_TASK_SET = "UN_WORK_TASK_SET"

    # 在线任务编号列表Set集合Key 任务编号集合
    TASK_ID_ONLINE_SET_WEB = "TASK_ID_ONLINE_SET_WEB"

    # Web前端存储设备对象
    TASK_ONLINE_SET_WEB = "TASK_ONLINE_SET_WEB"

    # 单个命令 命令在线集合
    ORDER_ONLINE_SET = "ORDER_ONLINE_SET"

    # 单个命令 命令在线集合
    ORDER_ONLINE_NETTY_SET = "ORDER_ONLINE_NETTY_SET"

    # 单个任务信息KEY 前缀在线状态
    TASK_ONLINE_PREFIX = "TASK_ONLINE_PREFIX"

    # 单个任务信息KEY 前缀在线状态
    BLOCK_TASK_ONLINE_PREFIX = "BLOCK_TASK_ONLINE_PREFIX"

    # 单个任务信息KEY 前缀在线状态
    TASK_ID_ONLINE_PREFIX = "TASK_ID_ONLINE_PREFIX"

    # 单个任务信息KEY 前缀在线状态
    HETERO_TASK_ONLINE_PREFIX = "HETERO_TASK_ONLINE_PREFIX"

    # 单个工作任务信息KEY 前缀在线工作状态
    WORK_TASK_ONLINE_PREFIX = "WORK_TASK_ONLINE_PREFIX"

    # 单个工作任务信息KEY 前缀未工作状态
    UN_WORK_TASK_PREFIX = "UN_WORK_TASK_PREFIX"

    # 单个工作子任务延时记录KEY 前缀未工作状态
    UN_WORK_SUB_TASK_KEY = "UN_WORK_SUB_TASK_KEY"

    # Web前端单个任务信息KEY 前缀在线状态
    TASK_ONLINE_PREFIX_WEB = "TASK_ONLINE_PREFIX_WEB"

    # 前端存储设备对象
    DEVICE_OBJ_ONLINE_SET = "DEVICE_OBJ_ONLINE_SET"

    # 前端存储设备对象(netty)
    DEVICE_OBJ_NETTY_SET = "DEVICE_OBJ_NETTY_SET"

    # 单个命令 前缀在线工作状态
    ORDER_ONLINE_PREFIX = "ORDER_ONLINE_PREFIX"

    # 单个命令 前缀在线工作状态
    ORDER_ONLINE_NETTY_PREFIX = "ORDER_ONLINE_NETTY_PREFIX"

    # 设备平均算力
    DEVICE_AVERAGECPU_LIST = "DEVICE_AVERAGECPU_LIST"
