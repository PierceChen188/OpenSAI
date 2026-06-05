__all__ = ['Algorithm_Train']

import torch
import torch.nn as nn
import torch.optim as optim
import torchvision
import torchvision.transforms as transforms
from torch.utils.data import Subset, DataLoader
import torch.nn.functional as F
import time
import torch.multiprocessing as mp
from tqdm import tqdm


class Algorithm_Train(nn.Module):
    def __init__(self, param_dict=None, data_dict=None):
        super(Algorithm_Train, self).__init__()
        self.param_dict = param_dict
        self.scale = self.param_dict['scale_num'] if 'scale_num' in self.param_dict else 10000
        self.num_epochs = self.param_dict['num_epochs'] if 'num_epochs' in self.param_dict else 50
        self.out_data_pth = self.param_dict['outDataPath'] if 'outDataPath' in  self.param_dict else './data/'
        self.in_data_pth = self.param_dict['inDataPath'] if 'inDataPath' in  self.param_dict else './data/'
        self.model_name = self.param_dict['model_name'] if 'model_name' in  self.param_dict else 'model.pth'

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
        mp.set_start_method("spawn", force=True)

        transform = transforms.Compose(
            [transforms.ToTensor(),
             transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))])

        # 加载训练集和测试集
        train_data = torchvision.datasets.CIFAR10(root=self.in_data_pth, train=True, download=True, transform=transform)
        # 使用 Subset 来创建一个只包含前50张图片的新数据集
        indices = list(range(50))
        subset_trainset = Subset(train_data, indices)
        trainloader = DataLoader(subset_trainset, batch_size=1, shuffle=False, num_workers=0)

        print("训练集的长度:{}".format(len(trainloader)))
        model = Algorithm_Train(self.param_dict)
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=0.001)

        start_time = time.time()
        print('start training')

        for epoch in range(self.num_epochs):
            running_loss = 0.0
            for data in trainloader:
                inputs, labels = data
                optimizer.zero_grad()
                outputs = model(inputs)
                loss = criterion(outputs, labels)
                loss.backward()
                optimizer.step()

                running_loss += loss.item()

            print(f'Epoch [{epoch + 1}/{self.num_epochs}], Loss: {running_loss / len(trainloader):.4f}')

            elapsed_time = int(time.time() - start_time)
            if elapsed_time > int(self.scale):  # 如果训练时间超过 scale 限制，提前停止
                print(elapsed_time)
                print("Training stopped early due to time limit.")
                break

        print('Finished Training')
        torch.save(model.state_dict(), self.out_data_pth+self.model_name)
        print('Model saved')
        return "Finished Training"