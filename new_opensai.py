# 1. 根据具体流程明确每一个模块的输入输出
# 1.1 产生request(request由离线算法产生)
# 1.2 车辆收到request之后回复reply（reply同样离线产生）
# 1.3 基础设施接收reply后选择当前时间戳进行通信的车辆ID：设备选择并分配通信时间
# 1.4 发数据，应用配置，需要网络信息，需要应用的profile
# 1.5 接收数据并返回协同感知结果


# 1. 控制平面
class NodeSelection():
    def __init__(self, Process, NetInfo, Comm):
        self.Process = Process
        self.NetInfo = NetInfo
        self.Comm = Comm

    def generate_request(self):
        # input: 当前时间戳
        # output: 产生的request
        # 过程：利用训练好的where2comm方法，把产生的request存下来，读取后发给车辆
        self.Process.read_request()
        self.Comm.send_data("vehicle","request")

    def generate_reply(self):
        # input: 基础设施发来的消息（包含request，时间戳）
        # output: 车辆的reply，包括是否包含基础设施C1所需要的数据,车辆位置，自身通信速率，车辆ID等，其中的attention map可以直接读取,发回reply
        self.Process.read_attention()
        self.Comm.send_data("vehicle", "reply")

    def select_vehicle(self):
        # input: N个车辆的reply：当前自身点云数据、当前周边车辆位置
        # output: 选择的车辆ID以及对应的通信资源分配策略
        # 主要策略：根据当前网络状态以及attention map判断与哪几辆车通信，并分配通信资源，即每一辆车在本周期内可以上传的
        self.NetInfo.report_net_info()
        # 设计策略：3辆车


class AppConfig():
    def __init__(self, Process):
        self.Process = Process
        
    def app_config(self):
        # input: 基础设施返回的资源分配结果
        # output: 根据离线产生的confidence map对中融合进行配置，即调整通信阈值，在资源受限的情况下保证识别准确率
        # 设计策略：给定一个通信量，应用配置(调整通信阈值)
        pass


# 管理信息 Management Information
class NetInfo():
    # 定期收集设备的网络状态，比如当前可用带宽，丢包情况
    def collect_net_info(self):
        pass
    def report_net_info(self):
        print("report net info")
        


class AppProfile():
    # 收集应用参数，比如三种融合方式的时延、准确率等
    def app_profile(self):
        pass


# 2. 数据平面
class Aggregator():
    def perception():
        # input: 车辆发过来的中融合特征
        # output: 调用feature fusion方法，可以启发式的，比如加和、平均值之类的，后续看是否可以调用训练好的网络
        pass
    def feature_fusion():
        # 1. 启发式方法 2. 调用训练好的网络
        pass
    # 1. 加和 2. 平均值 3. 最大值
    def detection_head():
        # 根据融合后的feature计算准确率
        pass
    # C H W，算准确率


class PreProcess():
    def read_feature():
        """
        根据index读取对应的点云文件,参考对应数据集的data loader,即进主模型之前做了哪些处理
        input: index,比如时刻为0时对应index为0,时刻为100ms时index为1
        output: 数据,比如batch_data,包含点云数据,ground truth,车辆位置等
        """
        print("read feature")

    def read_request(self, file_name):
        # 读取已经提取好的特征，C H W
        # 写一个读取txt文件的代码：输入：文件名 输出：H W 一个Tensor
        print("read request")
    
    def read_attention(self):
        print("read saved attention map")
    
    


class SendRecv():
    # 使用grpc，将经过PreProcess模块的数据发出
    def send_data(self, dest, data):
        # (1) request (2) reply (3) allocation decision (4) feature
        print("send {} to {}".format(data, dest))

    # 使用grpc,接收其他设备发来的数据
    def recv_data():
        pass


if __name__ == "__main__":
    process = PreProcess()
    netinfo = NetInfo()
    comm = SendRecv()
    node = NodeSelection(process,netinfo,comm)
    
    app_config = AppConfig(process)

    node.generate_request()
    node.generate_reply()
    node.select_vehicle()
    app_config.app_config()
    
# 1. 下周：where2comm 理解，代码
# 2. 2月份，实现最基础版本的代码，策略还不成熟
# 3. 3月份，优化策略
# 贡献：1. 平台 2. 两个优化策略
