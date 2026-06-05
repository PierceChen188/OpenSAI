## DAG Offloading 中使用的消息格式

### msg type
'''
{key->str : value->dict}
key type:
    "heart_beat": 边缘卸载设备的连接心跳；
    "user_beat": 用户设备的连接
    "dag_subtask_rsp": 卸载任务完成后的回传消息
    "udp_p2p_msg": 通过P2P方式发送UDP报文消息到指定client
    "udp_bc_msg": 通过BROADCAST方式发送UDP报文消息广播到到指定clients群组
    "devicesInfo": server连接到的所有devices的状态信息
    "dagData": 用户请求卸载DAG的格式数据
    "dagUpdate": 用户更新DAG完成进度的请求
    "devicesInfo": 用户获取devices info的请求
    "algorithmsInfo": 用户获取算法 info的请求
    "policiesInfo": 用户获取 策略info的请求
'''

### DAG msg request   用户请求卸载DAG的格式数据
'''
dag_data = {'dagData':
    {'taskName': 'app1',            # 任务名
    'taskId': '1',                  # 任务id
    'taskDesc': '协同感知任务',       # 任务描述
    'taskCreateTime': time.time(), #生成时间
    'taskPolicy': '策略1',          # 策略名
    'nodes':                       # 子任务组
    [{
        'id': 1,                   # 子任务id
        'name': 'Node1',           # 子任务名称
        'form': {
            'beforeNodes': [],     # 前置子任务
            'afterNodes': [],     # 后置子任务
            'taskAlgorithm': 'Algorithm_Pi_Calculation',    # 算法名称
            'taskVersion': '1.0',                  # 算法版本
            'taskInputInfo': {"inDataPath": '待执行任务取数据位置', "outDataPath":'待执行任务产生数据的位置' ,"count_num":100000},  
             # 算法函数的输入参数，可自定义dict，如count_num为algorithm_taskPi的输入参数，执行100000次pi计算
            'details': '描述子任务信息1',
            'deviceParam': {}
            },
        'result': {
            "taskResultInfo":'任务结果',    # 算法回传结果
            'status': 1,                  # 子任务状态
            'delay': 0,                   # 任务时延
            'deviceId': 1001              # 卸载设备id号
            }
    }]
}}
'''






### Device msg
'''
{
    'type' : "device_info",
    "devices":  [{
    'device_no': '73157e70-ca06-4835-b889-504ceb80208a',  # 1
    'type': 0,                   # 1   设备类型 enum:{LOCAL:0, VEHICLE:1, RSU:2, DEVICE:3, UN_KNOWN:4}
    'used_cpu': '1.3%',          # 1   cpu利用率
    'core_num': 20,
    'idle_memory': 2.99,         # 1   空闲存储空间（GB）
    'used_memory': 12.81,
    'total_memory': 15.8,
    'work_status': 0,
    'lng': 0.0,                   # 1   经度
    'lat': 0.0,                   # 1   纬度
    'order_value': -1,
    'ip_address': '127.0.0.1',    # 1   ip地址
    'throughput': 0,
    'bandwidth': 0,               # 1   传输带宽
    'network_usage': 0,
    'delay': 0,
    'port': 49452
    }
    ]
}
'''





## class form
'''
task_info
    task_id = ""    # 任务编号
    task_name = ""    # 任务名称
    task_location = ""    # 任务位置
    task_version = ""    # 任务版本
    task_uploader = ""    # 任务上传者
    task_algorithm = None    # 任务算法
    task_data_list = None    # 任务数据
    task_result_info = None    # 任务结果信息
    task_status = WorkStatus.UN_WORK    # 任务状态
    sub_task_id = []    # 子任务编号集
    device_param = {}    # 设备性能选择：
        # (1)高性能设备：device_param{key,value} =<"high_per",0/1>
        # (2)低时延：device_param{key,value} = < "Low_latency", 0 / 1 >
        # (3)大内存：device_param{key,value} = < "large_mem", 0 / 1 >
        # (4)设备数量：device_param{key,value} = <"device_num", x>

task_state_info
    task_status = WorkStatus.UN_WORK    # 任务状态
    task_start_time = None    # 任务开始时间
    task_run_time = None    # 任务运行时间
    sub_task_state_info = None    # 子任务状态信息
    sub_task_count = 0    # 子任务个数     

sub_task_info
    task_id = ""    # 任务编号
    sub_task_id = ""    # 子任务编号
    sub_task_name = ""    # 任务名称
    sub_task_version = ""    # 任务版本
    sub_task_location = ""    # 任务位置
    task_algorithm = None    # 任务算法
    sub_task_data_list = []    # 任务数据
    sub_task_result_info = None    # 任务结果信息
    sub_task_status = None    # 任务状态
    sub_task_count = 0    # 子任务个数
    run_before_list = []    # 被依赖的节点（后继）
    dependence_list = []    # 依赖的节点信息（前驱）
    device_info = None    # 选择卸载到的device节点信息

sub_task_status_info
    sub_task_id = ""    # 子任务编号
    device_id = ""    # 子任务所在设备编号
    sub_task_status = None    # 子任务状态


result_info
    device_no = ""    # 设备编号
    task_id = ""    # 任务编号
    sub_task_id = ""    # 子任务编号
    algo_name = ""    # 算法名称
    result_dict = {}    # 计算结果
    result_url = []    # 计算结果url(给出结果网页链接)

'''

