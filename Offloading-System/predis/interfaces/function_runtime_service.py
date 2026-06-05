class FunctionRunTimeService:
    def write_function_runtime_to_cache(self, task_id: str, function_name: str, run_time: int):
        """
        把某函数的运行时间写入緩存
        :param task_id: 任务编号
        :param function_name: 函数名称
        :param run_time: 运行时间
        :return: 无
        """
        pass

    def read_function_runtime(self, task_id: str, function_name: str) -> int:
        """
        从缓存中读出某函数的运行时间
        :param task_id: 任务编号
        :param function_name: 函数名称
        :return: 运行时间
        """
        pass
