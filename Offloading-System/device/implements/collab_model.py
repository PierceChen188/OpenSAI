"""协同感知 demo 的共享模型结构（被算法与训练脚本共用，保证权重可互相加载）。

场景：把一张 32x32 的 CIFAR 图按列分成左右两半（各 32x16），模拟“两台设备/两个传感器
各看到场景的一半”。每半经同一个 Backbone 提取 FEAT_DIM 维特征；FusionHead 把两份特征
拼接后分类。特征在设备间通过数据面(QUIC)传输。

注意：本文件不以 Algorithm_ 开头，不会被 actuator 当作可调用算法扫描到。
"""
import os

import torch
import torch.nn as nn
import torch.nn.functional as F

FEAT_DIM = 64
HALF_W = 16  # 32x32 图，每个 agent 看 32x16 的一半
CLASSES = ['airplane', 'automobile', 'bird', 'cat', 'deer',
           'dog', 'frog', 'horse', 'ship', 'truck']


class Backbone(nn.Module):
    """对一个 3x32x16 的半图提特征 -> FEAT_DIM 维向量。"""

    def __init__(self):
        super().__init__()
        self.c1 = nn.Conv2d(3, 16, 3, padding=1)
        self.c2 = nn.Conv2d(16, 32, 3, padding=1)
        self.fc = nn.Linear(32 * 8 * 4, FEAT_DIM)

    def forward(self, x):  # x: (B, 3, 32, 16)
        x = F.max_pool2d(F.relu(self.c1(x)), 2)   # (B,16,16,8)
        x = F.max_pool2d(F.relu(self.c2(x)), 2)   # (B,32,8,4)
        x = torch.flatten(x, 1)                   # (B,1024)
        return F.relu(self.fc(x))                 # (B,FEAT_DIM)


class FusionHead(nn.Module):
    """把 n_agents 份特征拼接后分类。"""

    def __init__(self, n_agents=2, n_classes=10):
        super().__init__()
        self.fc1 = nn.Linear(FEAT_DIM * n_agents, 64)
        self.fc2 = nn.Linear(64, n_classes)

    def forward(self, feats):  # feats: (B, FEAT_DIM*n_agents)
        return self.fc2(F.relu(self.fc1(feats)))


def split_halves(img):
    """img: (3,32,32) -> (left, right)，各 (3,32,16)。"""
    return img[:, :, :HALF_W], img[:, :, HALF_W:]


def data_dir():
    """返回 Offloading-System/data（CIFAR 数据与权重都在这里）。"""
    here = os.path.dirname(os.path.abspath(__file__))            # device/implements
    return os.path.join(os.path.dirname(os.path.dirname(here)), 'data')


BACKBONE_PATH = os.path.join(data_dir(), 'collab_backbone.pth')
FUSION_PATH = os.path.join(data_dir(), 'collab_fusion.pth')
