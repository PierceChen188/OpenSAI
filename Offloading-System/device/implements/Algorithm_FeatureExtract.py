__all__ = ['Algorithm_FeatureExtract']

import os

import numpy as np
import torch
import torchvision
import torchvision.transforms as T

from device.implements.collab_model import Backbone, BACKBONE_PATH, data_dir, split_halves

# 进程内缓存（首个任务加载，之后复用，避免每次重载 torch/数据/权重）
_BACKBONE = None
_TESTSET = None


class Algorithm_FeatureExtract:
    """协同感知：对 CIFAR 测试图的左/右半提取特征，写到 _artifact_out 供下游融合节点拉取。

    参数(taskInputInfo)：image_index(测试集下标), half('left'|'right')。
    """

    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
        global _BACKBONE, _TESTSET
        idx = int(self.param.get('image_index', 0))
        half = self.param.get('half', 'left')
        out = self.param.get('_artifact_out')
        if not out:
            return 'ERROR: 缺少 _artifact_out'

        root = data_dir()
        if _TESTSET is None:
            _TESTSET = torchvision.datasets.CIFAR10(root=root, train=False,
                                                    download=False, transform=T.ToTensor())
        if _BACKBONE is None:
            bb = Backbone()
            bb.load_state_dict(torch.load(BACKBONE_PATH, map_location='cpu'))
            bb.eval()
            _BACKBONE = bb

        img, label = _TESTSET[idx]          # img: (3,32,32)
        left, right = split_halves(img)
        h = left if half == 'left' else right
        with torch.no_grad():
            feat = _BACKBONE(h.unsqueeze(0))[0]   # (FEAT_DIM,)

        arr = feat.numpy().astype('float32')
        os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
        with open(out, 'wb') as f:
            np.save(f, arr)
        return f"feature[{half}] of image#{idx} (true_label={label}) extracted, dim={arr.shape[0]}"
