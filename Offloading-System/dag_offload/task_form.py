import asyncio
import websockets
import json

connected_clients = set()

async def notify_clients(mess):

    while True:
        for node in mess["nodes"]:
            node["result"]["status"] = (node["result"]["status"]+1) % 4

        message = json.dumps({"message": mess})
        print(message)
        if connected_clients:  # asyncio.wait doesn't accept an empty list
            await asyncio.wait([client.send(message) for client in connected_clients])
        await asyncio.sleep(5)


async def handler(websocket, path):
    # Register the client
    connected_clients.add(websocket)
    try:
        async for message in websocket:
            # Here we just accept and ignore any received messages
            print(message)
            pass
    except websockets.ConnectionClosed:
        pass
    finally:
        # Unregister the client
        connected_clients.remove(websocket)

async def main(mess):
    async with websockets.serve(handler, "localhost", 8000):
        await notify_clients(mess)  # Run the notification coroutine


# ------------- demo: feedback subtask result (use websocket)----------------

if __name__ == "__main__":
    test_data = {
        'type': "task_info",
        'createTime': "1719235688",  # 生成时间戳  unix timestamp
        'task_id': "1",  # 应用编号
        'detail_info': '描述应用任务的信息，如：协同感知任务',
        'edges': [],
        'nodes': [{
            'name': '算法A',  # 节点名称（类型）
            'iconClassName': 'el-icon-collection-tag',
            'id': 1,  # 任务 id
            'form': {
                'beforeNodes': [],  # 前置任务id
                'afterNodes': [2],  # 后置任务id

                'task_algorithm': "algorithm_taskA",  # 任务算法
                'task_version': "",  # 任务版本
                'task_data_list': "",  # 任务输入数据位置
                'task_result_info': "",  # 任务结果信息
                'task_location': "",  # 任务执行位置
                'details': '描述节点类型，如：协同感知任务',
                'device_param': {}  # 设备性能选择类型（待定）
            },

            'result': {'status': 0,  # enum->{ UN_WORK:0, READY:1, SUCCESS:2, FAIL:-1 }
                       # 任务的处理状态(未就绪、就绪、成果、失败）
                       'delay': -1,  # 时延=任务完成时刻时间戳-生成时刻时间戳    unix timestamp
                       'device_id': -1,  # 该子任务选择的设备id
                       },
        },
            {
                'name': '算法B',  # 节点名称（类型）
                'iconClassName': 'el-icon-collection-tag',
                'id': 2,  # 任务 id
                'form': {
                    'beforeNodes': [1],  # 前置任务id
                    'afterNodes': [],  # 后置任务id

                    'task_algorithm': "algorithm_taskB",  # 任务算法
                    'task_version': "",  # 任务版本
                    'task_data_list': "",  # 任务输入数据位置
                    'task_result_info': "",  # 任务结果信息
                    'task_location': "",  # 任务执行位置
                    'details': '描述节点类型，如：协同感知任务',
                    'device_param': {}  # 设备性能选择类型（待定）
                },

                'result': {'status': 0,  # enum->{ UN_WORK:0, READY:1, SUCCESS:2, FAIL:-1 }
                           # 任务的处理状态(未就绪、就绪、成果、失败）
                           'delay': -1,  # 时延=任务完成时刻时间戳-生成时刻时间戳    unix timestamp
                           'device_id': -1,  # 该子任务选择的设备id
                           },
            }]
    }
    asyncio.run(main(test_data))