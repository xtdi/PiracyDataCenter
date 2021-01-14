import json
import os

class Baijiaxing(object):
    # 初始化方法
    def __init__(self):
        project_root_dir = os.path.dirname(os.path.realpath(__file__))  # 获取项目根目录
        conf_file_path = os.path.join(project_root_dir, "conf\\baijiaxing.json")  # 文件路径
        conf_file_path = "D:\\PycharmProjects\\PiracyDataCenter\\conf\\baijiaxing.json"
        with open(conf_file_path, "r", encoding="utf8") as file_handle:
            # json_str = file_handle.readlines()
            # json_object = json.dumps(json_str)
            baijiaxing_dic = json.load(file_handle)
            self.baijiaxinglist = baijiaxing_dic["list"]

    def is_one_of_baijiaxing(self,xing_str):
        if xing_str in self.baijiaxinglist:
            return True
        else:
            return False


