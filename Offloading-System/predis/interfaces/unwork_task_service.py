class UnWorkTaskService:

    def write_unwork_task_to_cache(self, sub_task_info):
        """
        将未工作的子任务信息写入缓存
        :param sub_task_info: 未工作子任务信息
        :return: None
        """
        pass

    def read_unwork_task_info(self):
        """
        将未工作的子任务信息从缓存读出
        :return: 所有任务下的所有未工作子任务
        """
        pass

    def add_unwork_task_info(self, sub_task_info):
        """
        添加一条未工作的子任务信息
        :param sub_task_info: 未工作子任务信息
        :return: 是否添加成功True or False
        """
        pass

    def delete_unwork_task_info(self, sub_task_info):
        """
        删除一条未工作的子任务信息
        :param sub_task_info: 未工作子任务信息
        :return: 是否删除成功True or False
        """
        pass

    def delete_unwork_task_info_by_id(self, task_id, sub_task_id):
        """
        删除一条未工作的子任务信息
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: 是否删除成功True or False
        """
        pass

    def modify_unwork_task_info(self, sub_task_info):
        """
        修改一条未工作的子任务信息
        :param sub_task_info: 未工作子任务信息
        :return: 是否修改成功True or False
        """
        pass

    def query_unwork_task_info(self, sub_task_info):
        """
        查询一条未工作的子任务信息
        :param sub_task_info: 未工作子任务信息
        :return: taskId下subTaskID对应的未工作子任务
        """
        pass

    def query_unwork_task_info_by_id(self, task_id, sub_task_id):
        """
        查询一条未工作的子任务信息
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: taskId下subTaskID对应的未工作子任务
        """
        pass

    def add_unwork_task_list(self, sub_task_info_list):
        """
        添加一个未工作的任务信息
        :param sub_task_info_list: 任务信息
        :return: 是否添加成功True or False
        """
        pass

    def delete_unwork_task_list(self, sub_task_info_list):
        """
        删除一条未工作的任务信息
        :param sub_task_info_list: 任务信息
        :return: 是否删除成功True or False
        """
        pass

    def delete_unwork_task_list_by_id(self, task_id):
        """
        删除一条未工作的子任务信息
        :param task_id: 任务编号
        :return: 是否删除成功True or False
        """
        pass

    def modify_unwork_task_list(self, sub_task_info_list):
        """
        修改一条未工作的任务信息
        :param sub_task_info_list: 任务信息
        :return: 是否修改成功True or False
        """
        pass

    def query_unwork_task_list(self, sub_task_info_list):
        """
        查询一条未工作的任务信息
        :param sub_task_info_list: 任务信息
        :return: taskId下所有subTaskID对应的未工作子任务
        """
        pass

    def query_unwork_task_list_by_id(self, task_id):
        """
        查询task_id下的未工作子任务信息
        :param task_id: 任务编号
        :return: taskId下所有subTaskID对应的未工作子任务，以list形式返回
        """
        pass

    def query_unwork_task_num(self, task_id):
        """
        获取taskID任务下未工作子任务个数
        :param task_id: 任务编号
        :return: taskID任务下未工作子任务个数
        """
        pass

    def query_unwork_task_key_set_num(self):
        """
        判断Cache中当前有几个未工作子任务信息
        :return: Cache中当前有几个未工作子任务信息
        """
        pass
