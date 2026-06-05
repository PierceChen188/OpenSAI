# Opensai调度框架

# 系统框架
![img.png](Offloading-System/dag_offload/static/img.png)





# 计算卸载模块的项目文件梳理 
* Offloading-System/
*   README.md - 本文件 
*   Jenkinsfile - 用以自动构建和测试的脚本
*   Dockerfile - 用以自动构建 Docker 镜像的脚本
*   requirements.txt - 依赖包文件
* Offloading-System/dag_offload/
* server.py  - 用户端（底座）：
      1.监听并维护接入的边缘设备，
      2.对user_client的消息进行卸载决策；
      3.根据决策分发至不同的device_client进行处理；
      4.连接redis记录相关信息；
*   user_client.py  -  用户端（应用层）：通过Flask框架与DAG WEB端进行交互，接受来自WEB端的DAG算法请求，并发送至server进行卸载决策；
*   device_client.py  -  边缘设备端：连接server，接收并处理来自server的子任务，返回result
*   task_form.py  -  设置task的格式（与前端之间进行交互设计）
*   device_form.py  -  设定device的消息格式（与前端之间进行交互设计）
*   offloading_brain.py - 智能化的卸载决策（负责具体的任务调度）

## 项目运行 
在文件夹 Offloading-System/ 下：
1. 安装依赖(pip)
         $ pip install -r requirements.txt

2. 启动redis服务器；（redis server即可）
    
3. 依次启动不同的设备服务：
   $ python server.py
   $ python device_client.py
   $ python user_client.py

4. 启动web服务框架，并开启前端页面.

5. 按流程 选择 + 定制 dag任务图结构，之后将通过相应的执行逻辑返回不同的结果； 
6. 查看任务日志，获取任务执行结果与任务处理流程。


## 算法配置
不同应用需要自行设计实现相应的算法文件，存放在 ./device/implements/ 文件夹下，参考demo：algorithm_taskA/taskB.py
如果有跨节点存取数据的需求，需要自行实现.py执行文件中数据的存取路径，可以通过dag节点间的配置，实现相应的文件存取；
配置nodes中每个node的'taskResultInfo'（存数据路径）和'taskLocation'（取数据路径）


补充——更换前端编译包的流程（非必须）：
在 DAG-WEB 项目下，使用npm run build 命令生成前端页面的编译文件（./dist文件夹下），
将./dist下的所有文件都复制到该项目的./web/static/下
按更新文件的名称修改./web/templates/index.html中.js和.css的路径
运行python app.py 即可成功


## 项目前端页面

![img.png](Offloading-System/dag_offload/static/img2.png)