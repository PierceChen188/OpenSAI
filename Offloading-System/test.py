#!/usr/bin/env python3
"""
容器复用系统演示脚本

演示场景：
1. 启动长时间任务1（Task_1），容器1创建
2. 任务1执行中，启动任务2、3（并行）
3. 任务2完成，容器2变空闲
4. 复用容器2执行新任务4
5. 调整各容器资源
6. 停止容器，回收资源
"""

import sys
import os
import time
import threading

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from device.interfaces.resource_control_interface import ResourceControlInterface


def demo_multiple_containers_parallel():
    """演示：多个容器并行执行不同任务"""
    
    controller = ResourceControlInterface()
    containers_created = []
    print("-" * 70)
    print("1.容器创建与执行任务测试：启动3个容器，执行不同的任务")
    print("-" * 70)
    
    # 并行启动3个任务
    for i, task_name in enumerate(['Algorithm_Pi_Calculation', 'Algorithm_CNN', 'Algorithm_Pi_Calculation'], 1):
        print(f"\n启动任务{i}: {task_name}")
        result = controller.start_task(
            task_name=task_name,
            cpu_cores=1.0 + i * 0.5,  # 逐步增加CPU
            memory_mb=512 + i * 100
        )
        container_id = result['result'].get('container_id')
        containers_created.append(container_id)
        print(f"容器ID: {container_id}")
        print(f"结果: {result['result']['taskResultInfo']}")
    
    print("-" * 70)
    print("2.容器资源查询测试")
    print("-" * 70)
    print("\n查看所有容器资源")
    resources_info = controller.get_containers_resources()
    containers = resources_info['result']['containers']
    for cid, info in containers.items():
        print(f"{cid}: CPU={info['cpu_cores']}核, 内存={info['memory_mb']}MB")

    print("-" * 70)
    print("3.容器资源调整测试")
    print("-" * 70)
    # 调整第一个容器的资源
    print(f"\n调整 {containers_created[0]} 的资源 (CPU->3.0 mem->2048)")
    controller.update_container_resources(
        container_id=containers_created[0],
        cpu_cores=3.0,
        memory_mb=2048
    )
    print("\n再次查看所有容器资源")
    resources_info = controller.get_containers_resources()
    containers = resources_info['result']['containers']
    for cid, info in containers.items():
        print(f"{cid}: CPU={info['cpu_cores']}核, 内存={info['memory_mb']}MB")
    # 停止第二个容器
    print("-" * 70)
    print("4.容器停止测试")
    print("-" * 70)
    print(f"\n停止 {containers_created[1]}")
    controller.stop_container(container_id=containers_created[1])
    print("容器已停止")
    
    # 查看空闲容器
    print("\n查询空闲容器")
    idle_result = controller.get_idle_containers()
    print(f"空闲容器: {idle_result['result'].get('idle_containers', [])}")
    
    # 复用空闲容器
    print("-" * 70)
    print("5.容器复用测试")
    print("-" * 70)
    idle_containers = idle_result['result'].get('idle_containers', [])
    if idle_containers:
        print(f"\n复用空闲容器 {idle_containers[0]} 执行新任务")
        result = controller.reuse_idle_container(
            container_id=idle_containers[0],
            task_name='Algorithm_Sleep_1'
        )
        print(f"结果: {result['result']['taskResultInfo']}")
        print(f"该容器已执行任务数: {result['result'].get('task_count', 1)}")
    


def main():
    """主函数"""

    
    try:
        demo_multiple_containers_parallel()

        
        
    except Exception as e:
        print(f"\n❌ 演示过程中出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
