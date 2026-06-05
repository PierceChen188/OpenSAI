class DataInfo:
    def __init__(self):
        # 数据DataID
        self.__data_id = ""
        # 数据文件名称
        self.__data_name = ""
        # 此文件的所有者ID
        self.__data_owner_id = ""
        # 文件类型
        self.__data_type = ""
        # 存储此文件的地址
        self.__data_location = ""

    def set_data_info(self, data_id: str, data_name: str, data_owner_id: str, data_type: str, data_location: str):
        self.__data_id = data_id
        self.__data_name = data_name
        self.__data_owner_id = data_owner_id
        self.__data_type = data_type
        self.__data_location = data_location

    def set_data_info_by_dict(self, data_info_dict: dict):
        self.__data_id = data_info_dict['data_id']
        self.__data_name = data_info_dict['data_name']
        self.__data_owner_id = data_info_dict['data_owner_id']
        self.__data_type = data_info_dict['data_type']
        self.__data_location = data_info_dict['data_location']

    def get_data_info_by_dict(self) -> dict:
        data_info_dict = {"data_id": self.get_data_id(), "data_name": self.get_data_name(),
                          "data_owner_id": self.get_data_owner_id(), "data_type": self.get_data_type(),
                          "data_location": self.get_data_location()}
        return data_info_dict

    def set_data_id(self, data_id: str):
        self.__data_id = data_id

    def get_data_id(self) -> str:
        return self.__data_id

    def set_data_name(self, data_name: str):
        self.__data_name = data_name

    def get_data_name(self) -> str:
        return self.__data_name

    def set_data_owner_id(self, data_owner_id: str):
        self.__data_owner_id = data_owner_id

    def get_data_owner_id(self) -> str:
        return self.__data_owner_id

    def set_data_type(self, data_type: str):
        self.__data_type = data_type

    def get_data_type(self) -> str:
        return self.__data_type

    def set_data_location(self, data_location: str):
        self.__data_location = data_location

    def get_data_location(self) -> str:
        return self.__data_location

    def __str__(self):
        return f"DataInfo(data_id={self.__data_id}, data_name={self.__data_name}, data_owner_id={self.__data_owner_id}, " \
               f"data_type={self.__data_type}, data_location={self.__data_location})"
