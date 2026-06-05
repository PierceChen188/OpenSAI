import json
import subprocess
import os
import time
import logging
import psutil
import threading
from device.device_monitor import get_gl_device_no

class ResourceControlInterface:
    """资源控制接口 - 提供给优化组件使用"""
    
    def __init__(self):
        # 使用设备运行期生成的全局设备号，而不是写死
        self.device_id = get_gl_device_no()
        self.image_tag = "opensai-offloading:latest"  # 使用项目本地Dockerfile构建的镜像
        # 跟踪正在运行的容器
        self.running_containers = {}  # {container_id: container_name}
        self.container_counter = 0  # 容器计数器，用于生成 container_1, container_2 等
        self.container_lock = threading.Lock()
        
        # ✨ 新增：容器状态管理（支持容器复用）
        self.container_status = {}  # {container_id: {status: 'idle'|'running', task_name: str, created_time: float, task_count: int}}
        self.idle_containers_queue = []  # 空闲容器队列 [container_id, ...]
    
    def get_idle_containers(self) -> dict:
        """
        查询所有空闲容器
        
        Returns:
            dict: {'result': {'idle_containers': ['container_1', 'container_2'], ...}}
        """
        try:
            with self.container_lock:
                idle_list = [cid for cid in self.idle_containers_queue 
                           if self.container_status.get(cid, {}).get('status') == 'idle']
            
            return {
                'result': {
                    'taskResultInfo': f"当前空闲容器: {idle_list}",
                    'status': 1,
                    'idle_containers': idle_list,
                    'count': len(idle_list),
                    'deviceId': self.device_id
                }
            }
        except Exception as e:
            logging.error(f"查询空闲容器失败: {e}")
            return {
                'result': {
                    'taskResultInfo': f"查询失败: {str(e)}",
                    'status': 0,
                    'idle_containers': [],
                    'count': 0,
                    'deviceId': self.device_id
                }
            }
    
    def reuse_idle_container(self, container_id: str, task_name: str, cpu_cores: float = None, memory_mb: int = None) -> dict:
        """
        复用空闲容器执行新任务（容器不销毁，继续复用）
        
        Args:
            container_id: 要复用的容器ID（如 'container_1'）
            task_name: 新的任务名称
            cpu_cores: 可选，新的CPU核心数（不提供则保持原有）
            memory_mb: 可选，新的内存限制（不提供则保持原有）
            
        Returns:
            dict: {'result': {..., 'container_id': 'container_1', 'reused': True}}
        """
        start_time = time.time()
        
        try:
            with self.container_lock:
                if container_id not in self.container_status:
                    return {
                        'result': {
                            'taskResultInfo': f"容器 {container_id} 不存在",
                            'status': 0,
                            'delay': 0,
                            'deviceId': self.device_id
                        }
                    }
                
                if self.container_status[container_id].get('status') != 'idle':
                    return {
                        'result': {
                            'taskResultInfo': f"容器 {container_id} 不是空闲状态（当前: {self.container_status[container_id].get('status')}）",
                            'status': 0,
                            'delay': 0,
                            'deviceId': self.device_id
                        }
                    }
                
                # 更新容器状态为运行中
                self.container_status[container_id]['status'] = 'running'
                self.container_status[container_id]['task_name'] = task_name
                self.container_status[container_id]['task_count'] = self.container_status[container_id].get('task_count', 0) + 1
                self.idle_containers_queue.remove(container_id)
            
            # 如果指定了新的资源限制，调整容器资源
            if cpu_cores is not None or memory_mb is not None:
                docker_container_name = self.running_containers[container_id]
                if cpu_cores is not None:
                    update_cmd = ['docker', 'update', f'--cpus={cpu_cores}', docker_container_name]
                    subprocess.run(update_cmd, capture_output=True, timeout=10)
                    # ✅ 同时更新内存中的状态
                    with self.container_lock:
                        self.container_status[container_id]['cpu_cores'] = cpu_cores
                if memory_mb is not None:
                    update_cmd = ['docker', 'update', f'--memory={memory_mb}m', f'--memory-swap={memory_mb}m', docker_container_name]
                    subprocess.run(update_cmd, capture_output=True, timeout=10)
                    # ✅ 同时更新内存中的状态
                    with self.container_lock:
                        self.container_status[container_id]['memory_mb'] = memory_mb
            
            # 在容器中执行新任务
            task_result = self._execute_task_in_container(container_id, task_name)
            
            execution_time = time.time() - start_time
            
            # 执行完毕后更新容器状态为空闲
            with self.container_lock:
                self.container_status[container_id]['status'] = 'idle'
                self.idle_containers_queue.append(container_id)
            
            return {
                'result': {
                    'taskResultInfo': task_result,
                    'status': 1,
                    'delay': execution_time,
                    'deviceId': self.device_id,
                    'container_id': container_id,
                    'reused': True,  # 标记为容器复用
                    'task_count': self.container_status[container_id].get('task_count', 0)
                }
            }
            
        except Exception as e:
            logging.error(f"复用容器执行任务失败: {e}")
            execution_time = time.time() - start_time
            # 出错后恢复空闲状态
            with self.container_lock:
                if container_id in self.container_status:
                    self.container_status[container_id]['status'] = 'idle'
                    if container_id not in self.idle_containers_queue:
                        self.idle_containers_queue.append(container_id)
            return {
                'result': {
                    'taskResultInfo': f"复用容器失败: {str(e)}",
                    'status': 0,
                    'delay': execution_time,
                    'deviceId': self.device_id
                }
            }
    
    def _execute_task_in_container(self, container_id: str, task_name: str) -> str:
        """
        在指定容器中执行任务
        
        Args:
            container_id: 容器ID
            task_name: 任务名称
            
        Returns:
            str: 任务执行结果
        """
        try:
            docker_container_name = self.running_containers[container_id]
            
            # 使用 docker exec 在容器中执行Python脚本
            exec_cmd = [
                'docker', 'exec', docker_container_name,
                'python', '-c',
                self._generate_exec_task_script(task_name)
            ]
            
            result = subprocess.run(
                exec_cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    lines = output.split('\n')
                    # 获取最后一个非空行作为结果
                    for line in reversed(lines):
                        line = line.strip()
                        if line and not line.startswith('['):
                            return line
                return "任务已执行"
            else:
                return f"执行失败: {result.stderr}"
                
        except subprocess.TimeoutExpired:
            return "任务执行超时"
        except Exception as e:
            logging.error(f"在容器中执行任务失败: {e}")
            return f"执行异常: {str(e)}"
    
    def _generate_exec_task_script(self, task_name: str) -> str:
        """生成在容器中执行的任务脚本"""
        return f"""
import sys
sys.path.append('/app')

try:
    from device.implements.{task_name} import {task_name}
    param_dict = {{'count_num': 10000}}
    data_dict = {{}}
    algo = {task_name}(param_dict, data_dict)
    result = algo.run()
    print(str(result))
except Exception as e:
    print(f"Error: {{e}}")
"""
    
    def start_task(self, task_name: str, cpu_cores: float, memory_mb: int, param_dict: dict = None, data_dict: dict = None) -> dict:
        """
        启动容器化任务并立即返回执行结果（容器后台持续运行）
        
        Args:
            task_name: 任务名称，如 'Algorithm_Pi_Calculation'
            cpu_cores: CPU核心数，如 2.5
            memory_mb: 内存限制，如 4096MB
            
        Returns:
            dict: {'result': {"taskResultInfo": '任务执行结果', 'status': 1, 'delay': 0.27, 
                             'deviceId': '...', 'container_id': 'container_1'}}
                  容器启动后在后台继续运行，由用户手动停止
        """
        start_time = time.time()
        
        try:
            # 验证资源是否可用
            if not self._check_resources_available(cpu_cores, memory_mb):
                return {
                    'result': {
                        "taskResultInfo": f"资源不足: CPU {cpu_cores} 核心, 内存 {memory_mb}MB",
                        'status': 0,
                        'delay': 0,
                        'deviceId': self.device_id
                    }
                }
            
            # 启动容器并获取任务执行结果（容器后台持续运行）
            task_result, container_id, docker_container_id = self._start_and_execute_task(task_name, cpu_cores, memory_mb, param_dict, data_dict)
            
            execution_time = time.time() - start_time
            
            return {
                'result': {
                    "taskResultInfo": task_result,
                    'status': 1,
                    'delay': execution_time,
                    'deviceId': self.device_id,
                    'container_id': container_id  # 新增：容器ID如 container_1
                }
            }
            
        except Exception as e:
            logging.error(f"任务启动失败: {e}")
            execution_time = time.time() - start_time
            return {
                'result': {
                    "taskResultInfo": f"启动失败: {str(e)}",
                    'status': 0,
                    'delay': execution_time,
                    'deviceId': self.device_id
                }
            }
    
    def execute_task_with_resources(self, task_name: str, cpu_cores: float, memory_mb: int) -> dict:
        """
        在指定资源限制下执行任务（阻塞式，等待任务完成）
        
        Args:
            task_name: 任务名称，如 'Algorithm_Pi_Calculation'
            cpu_cores: CPU核心数，如 2.5
            memory_mb: 内存限制，如 4096MB
            
        Returns:
            dict: {'result': {"taskResultInfo": '任务结果', 'status': 1, 'delay': 0, 'deviceId": ...}}
        """
        start_time = time.time()
        
        try:
            # 验证资源是否可用
            if not self._check_resources_available(cpu_cores, memory_mb):
                return {
                    'result': {
                        "taskResultInfo": f"资源不足: CPU {cpu_cores} 核心, 内存 {memory_mb}MB",
                        'status': 0,  # 失败状态
                        'delay': 0,
                        'deviceId': self.device_id
                    }
                }
            
            # 执行容器化任务
            result = self._run_containerized_task(task_name, cpu_cores, memory_mb)
            
            # 计算执行时间
            execution_time = time.time() - start_time
            
            return {
                'result': {
                    "taskResultInfo": result,
                    'status': 1,  # 成功状态
                    'delay': execution_time,
                    'deviceId': self.device_id
                }
            }
            
        except Exception as e:
            logging.error(f"任务执行失败: {e}")
            execution_time = time.time() - start_time
            return {
                'result': {
                    "taskResultInfo": f"执行失败: {str(e)}",
                    'status': 0,  # 失败状态
                    'delay': execution_time,
                    'deviceId': self.device_id
                }
            }
    
    def _check_resources_available(self, cpu_cores: float, memory_mb: int) -> bool:
        """检查资源是否可用"""
        try:
            available_memory = psutil.virtual_memory().available // (1024 * 1024)  # MB
            cpu_count = psutil.cpu_count()
            
            return cpu_cores <= cpu_count and memory_mb <= available_memory
        except Exception as e:
            logging.warning(f"资源检查失败: {e}")
            return True  # 如果检查失败，允许执行
    
    def _start_and_execute_task(self, task_name: str, cpu_cores: float, memory_mb: int, param_dict: dict = None, data_dict: dict = None) -> tuple:
        """
        启动容器、执行任务、返回结果，容器后台持续运行
        
        实现逻辑：
        1. 启动容器（在后台）
        2. 等待任务执行并获取结果
        3. 让容器继续运行（用户后续可手动停止）
        
        Returns:
            tuple: (任务执行结果, container_id, docker_container_id)
                  task_result: 执行结果
                  container_id: 简化的容器ID如 'container_1'
                  docker_container_id: Docker 返回的完整容器ID
        """
        # 生成简洁的容器ID
        with self.container_lock:
            self.container_counter += 1
            container_id = f"container_{self.container_counter}"
        
        # Docker 容器真实名称
        docker_container_name = f"{container_id}_{task_name}_{int(time.time())}"
        
        # 记录容器信息用于后续资源调整
        with self.container_lock:
            self.running_containers[container_id] = docker_container_name
        
        try:
            # 确保本地镜像存在，不存在则自动构建
            self._ensure_image(self.image_tag)

            # 计算项目根目录
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

            # 数据目录（在项目根目录的上一级）
            data_root = os.path.join(os.path.dirname(project_root), 'data')

            # 构建Docker命令 - 启动容器执行任务，但容器持续运行
            docker_cmd = [
                'docker', 'run', '-d',
                f'--cpus={cpu_cores}',
                f'--memory={memory_mb}m',
                f'--name={docker_container_name}',
                '-v', f'{project_root}:/app',
                '-v', f'{data_root}:/data',  # 挂载数据目录
                '-w', '/app',
                '-e', 'PYTHONPATH=/app',
                '--entrypoint', 'python',
                self.image_tag,
                '-c', self._generate_task_with_persistence_script(task_name, param_dict, data_dict)
            ]
            
            # 启动容器
            result = subprocess.run(
                docker_cmd, 
                capture_output=True, 
                text=True, 
                timeout=30,
                cwd=project_root
            ) 

            if result.returncode != 0:
                with self.container_lock:
                    self.running_containers.pop(container_id, None)
                raise RuntimeError(f"容器启动失败: {result.stderr}")
            
            docker_container_id = result.stdout.strip()

            # 等待任务执行并获取结果
            task_result = self._wait_for_task_result(docker_container_name, timeout=300)

            # 清理已退出的容器
            try:
                subprocess.run(['docker', 'rm', '-f', docker_container_name],
                             capture_output=True, timeout=10)
            except Exception as e:
                logging.warning(f"清理容器失败: {e}")

            # 从运行容器列表中移除
            with self.container_lock:
                self.running_containers.pop(container_id, None)

            return task_result, container_id, docker_container_id
                
        except subprocess.TimeoutExpired:
            with self.container_lock:
                self.running_containers.pop(container_id, None)
            raise RuntimeError("任务执行超时")
        except Exception as e:
            with self.container_lock:
                self.running_containers.pop(container_id, None)
            raise RuntimeError(f"执行异常: {str(e)}")
    
    def _wait_for_task_result(self, container_name: str, timeout: int = 300) -> str:
        """
        等待容器执行任务并获取结果

        通过 docker logs 监听容器输出，直到找到结果标记或容器停止
        """
        start_time = time.time()
        result_found = False
        task_result = ""
        last_log = ""

        try:
            while time.time() - start_time < timeout:
                # 获取容器日志
                log_cmd = ['docker', 'logs', container_name]
                log_result = subprocess.run(
                    log_cmd,
                    capture_output=True,
                    text=True,
                    timeout=10
                )

                if log_result.returncode == 0:
                    logs = log_result.stdout.strip()

                    if logs and logs != last_log:
                        last_log = logs

                    # 查找结果行（查找包含"任务执行完成，结果:"的行）
                    if logs:
                        lines = logs.strip().split('\n')
                        # 从后往前找，避免匹配到调试信息
                        for line in reversed(lines):
                            if '任务执行完成，结果:' in line and not line.strip().startswith('print'):
                                task_result = line.split('任务执行完成，结果:')[-1].strip()
                                result_found = True
                                break

                        # 如果没找到结果行，检查是否有错误
                        if not result_found:
                            for line in reversed(lines):
                                line_stripped = line.strip()
                                if 'ERROR' in line_stripped or '异常' in line_stripped or 'Error' in line_stripped:
                                    task_result = line_stripped
                                    result_found = True
                                    break

                    if result_found:
                        break
                else:
                    logging.warning(f"获取容器日志失败: {log_result.stderr}")

                # 检查容器是否还在运行
                check_cmd = ['docker', 'ps', '--filter', f'name={container_name}', '--format', '{{.Names}}']
                check_result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=5)

                if not check_result.stdout.strip():
                    # 容器已停止，尝试最后一次获取日志
                    log_cmd = ['docker', 'logs', container_name]
                    log_result = subprocess.run(log_cmd, capture_output=True, text=True, timeout=10)
                    if log_result.returncode == 0:
                        logs = log_result.stdout.strip()
                        if logs:
                            lines = logs.strip().split('\n')
                            # 从后往前找
                            for line in reversed(lines):
                                if '任务执行完成，结果:' in line and not line.strip().startswith('print'):
                                    task_result = line.split('任务执行完成，结果:')[-1].strip()
                                    break

                            # 如果没找到结果，检查是否有错误信息
                            if not task_result:
                                for line in reversed(lines):
                                    line_stripped = line.strip()
                                    if 'ERROR' in line_stripped or '异常' in line_stripped or 'Error' in line_stripped:
                                        task_result = line_stripped
                                        break
                    break

                time.sleep(0.5)

            if not task_result:
                logging.warning(f"等待超时或没有找到结果，最后看到的日志:\n{last_log}")
                task_result = "任务已启动（结果等待中）"

            return task_result

        except Exception as e:
            logging.error(f"获取任务结果失败: {e}")
            return "任务已启动（结果获取异常）"
    
    def _run_containerized_task(self, task_name: str, cpu_cores: float, memory_mb: int) -> str:
        """运行容器化任务"""
        container_name = f"task_{task_name}_{int(time.time())}"
        result = None  # 初始化 result 变量

        try:
            # 确保本地镜像存在，不存在则自动构建
            self._ensure_image(self.image_tag)

            # 计算项目根目录（当前文件位于 /device/interfaces/ 下）
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

            # 构建Docker命令
            docker_cmd = [
                'docker', 'run', '--rm',
                f'--cpus={cpu_cores}',
                f'--memory={memory_mb}m',
                f'--name={container_name}',
                # 挂载项目目录，确保使用最新代码
                '-v', f'{project_root}:/app',
                '-w', '/app',
                '-e', 'PYTHONPATH=/app',
                '--entrypoint', 'python',
                self.image_tag,
                '-c', self._generate_task_script(task_name)
            ]

            #执行容器
            result = subprocess.run(
                docker_cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5分钟超时
                cwd=project_root
            )


            if result.returncode == 0:
                return result.stdout.strip()
            else:
                return f"容器执行失败: {result.stderr}"

        except subprocess.TimeoutExpired:
            return "任务执行超时"
        except Exception as e:
            return f"执行异常: {str(e)}"

        finally:
            if result is not None and result.returncode != 0:  # 只在任务失败时清理容器
                try:
                    subprocess.run(['docker', 'rm', '-f', container_name],
                                 capture_output=True, timeout=5)
                except:
                    pass
    
    def _generate_task_script(self, task_name: str) -> str:
        """生成简单任务脚本（用于execute_task_with_resources）"""
        return f"""
import sys
sys.path.append('/app')

try:
    from device.implements.{task_name} import {task_name}
    param_dict = {{'count_num': 10000}}
    data_dict = {{}}
    algo = {task_name}(param_dict, data_dict)
    result = algo.run()
    print(str(result))
except Exception as e:
    print(f"Error: {{e}}")
    import traceback
    traceback.print_exc()
"""

    def _generate_persistent_task_script(self, task_name: str, param_dict: dict = None, data_dict: dict = None) -> str:
        """生成任务脚本（执行任务后立即退出）"""
        # 处理默认参数
        if param_dict is None:
            param_dict = {'count_num': 10000}
        if data_dict is None:
            data_dict = {}

        # 将字典转换为字符串（用于脚本生成）
        param_dict_str = json.dumps(param_dict)
        data_dict_str = json.dumps(data_dict)

        # 使用普通字符串拼接，避免f-string嵌套导致的变量解析问题
        script = """
import sys
import json

sys.path.append('/app')

try:
    from device.implements.TASK_NAME import TASK_NAME

    param_dict = json.loads('''PARAM_DICT_STR''')
    data_dict = json.loads('''DATA_DICT_STR''')

    algo = TASK_NAME(param_dict, data_dict)

    try:
        result = algo.run()
        print(f"任务执行完成，结果: {result}", flush=True)
    except Exception as run_error:
        import traceback
        traceback.print_exc()
        print(f"任务执行完成，结果: ERROR - {str(run_error)}", flush=True)

except KeyboardInterrupt:
    print("任务被中断", flush=True)
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"任务执行完成，结果: ERROR - {str(e)}", flush=True)
"""
        # 替换占位符
        script = script.replace('TASK_NAME', task_name)
        script = script.replace('PARAM_DICT_STR', param_dict_str)
        script = script.replace('DATA_DICT_STR', data_dict_str)

        return script

    def _generate_task_with_persistence_script(self, task_name: str, param_dict: dict = None, data_dict: dict = None) -> str:
        """生成任务脚本（执行一次后容器保活）"""
        return self._generate_persistent_task_script(task_name, param_dict, data_dict)

    def _ensure_image(self, image_tag: str) -> None:
        """确保本地存在指定镜像；若不存在则基于项目根目录的Dockerfile自动构建"""
        try:
            # 查询镜像是否存在
            inspect = subprocess.run(
                ['docker', 'images', '-q', image_tag],
                capture_output=True, text=True, timeout=30
            )
            if inspect.returncode == 0 and inspect.stdout.strip():
                return

            # 未找到则构建
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            build = subprocess.run(
                ['docker', 'build', '-t', image_tag, '.'],
                capture_output=True, text=True, timeout=1200, cwd=project_root
            )
            if build.returncode != 0:
                raise RuntimeError(f"构建镜像失败: {build.stderr}")
        except Exception as e:
            raise RuntimeError(f"检查/构建镜像失败: {e}")

    def update_container_resources(self, container_id: str, cpu_cores: float, memory_mb: int) -> dict:
        """
        动态调整正在运行的容器资源
        
        Args:
            container_id: 容器ID（如 'container_1'）
            cpu_cores: 新的CPU核心数
            memory_mb: 新的内存限制（MB）
            
        Returns:
            dict: 调整结果
        """
        try:
            with self.container_lock:
                docker_container_name = self.running_containers.get(container_id)
            
            if not docker_container_name:
                return {
                    'result': {
                        "taskResultInfo": f"未找到容器 {container_id}",
                        'status': 0,
                        'delay': 0,
                        'deviceId': self.device_id
                    }
                }
            
            # 检查容器是否还在运行
            check_cmd = ['docker', 'ps', '--filter', f'name={docker_container_name}', '--format', '{{.Names}}']
            check_result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=10)
            
            if not check_result.stdout.strip():
                # 容器已停止，清理记录
                with self.container_lock:
                    self.running_containers.pop(container_id, None)
                return {
                    'result': {
                        "taskResultInfo": f"容器 {container_id} 已停止，无法调整资源",
                        'status': 0,
                        'delay': 0,
                        'deviceId': self.device_id
                    }
                }
            
            # 使用 docker update 命令动态调整资源
            update_cmd = [
                'docker', 'update',
                f'--cpus={cpu_cores}',
                f'--memory={memory_mb}m',
                f'--memory-swap={memory_mb}m',  # ✅ 同时设置 memoryswap 防止出错
                docker_container_name
            ]
            
            update_result = subprocess.run(
                update_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if update_result.returncode == 0:
                # ✅ 关键修复：同时更新内存中的 container_status
                with self.container_lock:
                    if container_id in self.container_status:
                        self.container_status[container_id]['cpu_cores'] = cpu_cores
                        self.container_status[container_id]['memory_mb'] = memory_mb
                
                return {
                    'result': {
                        "taskResultInfo": f"成功调整容器资源: CPU {cpu_cores} 核心, 内存 {memory_mb}MB",
                        'status': 1,
                        'delay': 0,
                        'deviceId': self.device_id
                    }
                }
            else:
                return {
                    'result': {
                        "taskResultInfo": f"资源调整失败: {update_result.stderr}",
                        'status': 0,
                        'delay': 0,
                        'deviceId': self.device_id
                    }
                }
                
        except Exception as e:
            logging.error(f"动态调整资源失败: {e}")
            return {
                'result': {
                    "taskResultInfo": f"资源调整异常: {str(e)}",
                    'status': 0,
                    'delay': 0,
                    'deviceId': self.device_id
                }
            }
    
    def stop_container(self, container_id: str) -> dict:
        """
        停止指定容器
        
        Args:
            container_id: 容器ID（如 'container_1'）
            
        Returns:
            dict: 停止结果
        """
        try:
            with self.container_lock:
                docker_container_name = self.running_containers.get(container_id)
            
            if not docker_container_name:
                return {
                    'result': {
                        "taskResultInfo": f"未找到容器 {container_id}",
                        'status': 0,
                        'delay': 0,
                        'deviceId': self.device_id
                    }
                }
            
            # 停止容器
            stop_cmd = ['docker', 'stop', docker_container_name]
            stop_result = subprocess.run(stop_cmd, capture_output=True, text=True, timeout=30)
            
            # 清理记录
            with self.container_lock:
                self.running_containers.pop(container_id, None)
                self.container_status.pop(container_id, None)  # ✨ 新增：清理状态
                if container_id in self.idle_containers_queue:  # ✨ 新增：从空闲队列移除
                    self.idle_containers_queue.remove(container_id)
            
            if stop_result.returncode == 0:
                return {
                    'result': {
                        "taskResultInfo": f"成功停止容器 {container_id}",
                        'status': 1,
                        'delay': 0,
                        'deviceId': self.device_id
                    }
                }
            else:
                return {
                    'result': {
                        "taskResultInfo": f"停止容器失败: {stop_result.stderr}",
                        'status': 0,
                        'delay': 0,
                        'deviceId': self.device_id
                    }
                }
                
        except Exception as e:
            logging.error(f"停止容器失败: {e}")
            return {
                'result': {
                    "taskResultInfo": f"停止容器异常: {str(e)}",
                    'status': 0,
                    'delay': 0,
                    'deviceId': self.device_id
                }
            }
    
    def get_running_containers(self) -> dict:
        """
        获取当前正在运行的容器列表及其状态
        
        Returns:
            dict: 容器列表和状态信息
        """
        try:
            with self.container_lock:
                containers = dict(self.running_containers)
                statuses = dict(self.container_status)
            
            # 构建详细信息
            container_info = {}
            for cid, docker_name in containers.items():
                status_info = statuses.get(cid, {})
                container_info[cid] = {
                    'docker_name': docker_name,
                    'status': status_info.get('status', 'unknown'),
                    'task_name': status_info.get('task_name', ''),
                    'task_count': status_info.get('task_count', 0),
                    'created_time': status_info.get('created_time', 0)
                }
            
            return {
                'result': {
                    "taskResultInfo": f"当前运行容器: {list(containers.keys())}",
                    'status': 1,
                    'delay': 0,
                    'deviceId': self.device_id,
                    'containers': container_info,
                    'idle_count': len(self.idle_containers_queue),  # ✨ 新增：空闲容器数
                    'running_count': len(containers) - len(self.idle_containers_queue)  # ✨ 新增：运行中容器数
                }
            }
        except Exception as e:
            logging.error(f"获取容器列表失败: {e}")
            return {
                'result': {
                    "taskResultInfo": f"获取容器列表异常: {str(e)}",
                    'status': 0,
                    'delay': 0,
                    'deviceId': self.device_id
                }
            }

    def get_containers_resources(self) -> dict:
        """
        获取所有容器的资源使用情况（CPU和内存）
        
        Returns:
            dict: 包含所有容器的资源信息
                {
                    'result': {
                        'containers': {
                            'container_1': {
                                'container_id': 'container_1',
                                'docker_container_name': '...',
                                'status': 'running' 或 'idle',
                                'cpu_cores': 1.0,
                                'memory_mb': 512,
                                'task_name': 'Algorithm_Pi_Calculation',
                                'task_count': 1,
                                'created_time': 1234567890.123
                            },
                            ...
                        },
                        'total_containers': 3,
                        'running_containers': 2,
                        'idle_containers': 1
                    }
                }
        """
        try:
            with self.container_lock:
                containers_info = {}
                
                for container_id, docker_container_name in self.running_containers.items():
                    status_info = self.container_status.get(container_id, {})
                    
                    # 获取容器的资源配置
                    cpu_cores = status_info.get('cpu_cores', 1.0)
                    memory_mb = status_info.get('memory_mb', 512)
                    
                    containers_info[container_id] = {
                        'container_id': container_id,
                        'docker_container_name': docker_container_name,
                        'status': status_info.get('status', 'unknown'),
                        'cpu_cores': cpu_cores,
                        'memory_mb': memory_mb,
                        'task_name': status_info.get('task_name', 'unknown'),
                        'task_count': status_info.get('task_count', 0),
                        'created_time': status_info.get('created_time', 0)
                    }
                
                # 计算统计信息
                running_count = len([c for c in containers_info.values() if c['status'] == 'running'])
                idle_count = len([c for c in containers_info.values() if c['status'] == 'idle'])
                
                return {
                    'result': {
                        'containers': containers_info,
                        'total_containers': len(containers_info),
                        'running_containers': running_count,
                        'idle_containers': idle_count,
                        'status': 1
                    }
                }
        except Exception as e:
            logging.error(f"获取容器资源信息失败: {e}")
            return {
                'result': {
                    'containers': {},
                    'total_containers': 0,
                    'running_containers': 0,
                    'idle_containers': 0,
                    'status': 0,
                    'error': str(e)
                }
            }

    def get_available_resources(self) -> dict:
        """
        获取当前设备可用资源（可选功能，供优化组件参考）
        
        Returns:
            dict: 可用资源信息
        """
        try:
            total_cpu = psutil.cpu_count()
            total_memory = psutil.virtual_memory().total // (1024 * 1024)  # MB
            available_memory = psutil.virtual_memory().available // (1024 * 1024)  # MB
            cpu_percent = psutil.cpu_percent(interval=1)
            
            return {
                'total_cpu_cores': total_cpu,
                'total_memory_mb': total_memory,
                'available_cpu_cores': total_cpu * (1 - cpu_percent / 100),
                'available_memory_mb': available_memory,
                'cpu_usage_percent': cpu_percent,
                'memory_usage_percent': psutil.virtual_memory().percent
            }
        except Exception as e:
            logging.error(f"获取资源信息失败: {e}")
            return {
                'total_cpu_cores': 4,
                'total_memory_mb': 8192,
                'available_cpu_cores': 4,
                'available_memory_mb': 4096,
                'cpu_usage_percent': 0,
                'memory_usage_percent': 0
            }
    
    def recycle_idle_containers(self, idle_timeout: int = 3600) -> dict:
        """
        回收长时间空闲的容器（释放资源）
        
        Args:
            idle_timeout: 空闲超时时间（秒），默认1小时
            
        Returns:
            dict: 回收结果
        """
        try:
            current_time = time.time()
            recycled = []
            
            with self.container_lock:
                containers_to_remove = []
                
                for container_id in self.idle_containers_queue[:]:
                    status_info = self.container_status.get(container_id, {})
                    created_time = status_info.get('created_time', current_time)
                    
                    if current_time - created_time > idle_timeout:
                        containers_to_remove.append(container_id)
                
                # 执行回收
                for container_id in containers_to_remove:
                    docker_container_name = self.running_containers.get(container_id)
                    if docker_container_name:
                        try:
                            subprocess.run(['docker', 'stop', docker_container_name], 
                                         capture_output=True, timeout=10)
                            subprocess.run(['docker', 'rm', docker_container_name], 
                                         capture_output=True, timeout=10)
                            recycled.append(container_id)
                            logging.info(f"回收容器: {container_id}")
                        except Exception as e:
                            logging.error(f"回收容器 {container_id} 失败: {e}")
                    
                    # 清理记录
                    self.running_containers.pop(container_id, None)
                    self.container_status.pop(container_id, None)
                    self.idle_containers_queue.remove(container_id)
            
            return {
                'result': {
                    'taskResultInfo': f"成功回收 {len(recycled)} 个容器: {recycled}",
                    'status': 1,
                    'recycled_containers': recycled,
                    'deviceId': self.device_id
                }
            }
            
        except Exception as e:
            logging.error(f"容器回收失败: {e}")
            return {
                'result': {
                    'taskResultInfo': f"回收失败: {str(e)}",
                    'status': 0,
                    'recycled_containers': [],
                    'deviceId': self.device_id
                }
            }


# 使用示例
if __name__ == '__main__':
    # 创建接口实例
    controller = ResourceControlInterface()
    
    # 查看可用资源
    resources = controller.get_available_resources()
    print(f"可用资源: {resources}")
    
    # 执行任务
    result = controller.execute_task_with_resources(
        task_name='Algorithm_Pi_Calculation',
        cpu_cores=2.0,      # 使用2个CPU核心
        memory_mb=1024      # 限制1GB内存
    )
    
    print(f"执行结果: {result}")
