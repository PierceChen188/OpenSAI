__all__ = ['Algorithm_NEW_CNN']

import os
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision
import torchvision.transforms as transforms
#from numba.core.ir import Print
from torch.utils.data import DataLoader
import torch.nn.functional as F
from tqdm import tqdm


class Algorithm_NEW_CNN(nn.Module):
    def __init__(self, param_dict, data_dict=None):
        super(Algorithm_NEW_CNN, self).__init__()

        self.param_dict = param_dict
        self.data_dict = data_dict
        self.scale = self.param_dict['scale_num'] if 'scale_num' in self.param_dict else 10
        self.out_data_pth = self.param_dict['outDataPath'] if 'outDataPath' in self.param_dict else './data/'
        self.in_data_pth = self.param_dict['inDataPath'] if 'inDataPath' in self.param_dict else './data/'
        self.model_name = self.param_dict['model_name'] if 'model_name' in self.param_dict else 'model.pth'

        self.conv1 = nn.Conv2d(3, 32, 3, padding=1)
        self.conv2 = nn.Conv2d(32, 64, 3, padding=1)
        self.conv3 = nn.Conv2d(64, 128, 3, padding=1)
        self.pool = nn.MaxPool2d(2, 2)
        self.fc1 = nn.Linear(128 * 4 * 4, 256)
        self.fc2 = nn.Linear(256, 128)
        self.fc3 = nn.Linear(128, 10)

    def forward(self, x):
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = self.pool(F.relu(self.conv3(x)))
        x = x.view(-1, 128 * 4 * 4)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x

    def run(self):
        total_num = self.param_dict['count_num'] if 'count_num' in self.param_dict else 10
        transform = transforms.Compose(
            [transforms.ToTensor(),
             transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))])

        # 加载测试集
        testset = torchvision.datasets.CIFAR10(root=self.in_data_pth, train=False, download=True, transform=transform)
        testloader = DataLoader(testset, batch_size=1, shuffle=False, num_workers=0)

        model = Algorithm_NEW_CNN(self.param_dict)

        base_dir = os.path.dirname(os.path.abspath(__file__))
        # model_path = os.path.join(base_dir, 'model.pth')
        # model.load_state_dict(torch.load(model_path))
        state_dict = torch.load(self.out_data_pth+self.model_name, map_location='cpu')
        model.load_state_dict(state_dict)
        model.eval()

        correct = 0
        total = 0
        #print('Start inference')

        with torch.no_grad():
            for i in range(total_num):
                images, labels =testset[i]
                outputs = model(images)
                _, predicted = torch.max(outputs.data, 1)  # 获取最大值索引
                #total += labels.size(0)
                correct += (predicted == labels).sum().item()

        # 计算准确率
        accuracy = 100 * correct / total_num
        #print(f'Accuracy: {accuracy:.2f}%')
        #print(f'推理图片数量: {total_num:}')
        result = f"Accuracy: {accuracy:.2f}%"
        return result