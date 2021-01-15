import json
import os
import copy

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


    def merget_data(self):
        file_path = "D:\\FFOutput\\中国姓氏大全包含2200个单姓.txt"
        need_add_list = []
        need_add_num = 0
        existed_num = 0
        total_num = 0
        with open(file_path, "r", encoding="utf8") as file_handle:
            while True:
                try:
                    cur_row = file_handle.readline()
                    if not cur_row:
                        break
                    cur_row = cur_row.replace("\t", "")

                    for everyChar in cur_row:
                        total_num = total_num + 1
                        cur_char = everyChar.strip()
                        if len(cur_char) > 0:
                            if cur_char in self.baijiaxinglist:
                                existed_num = existed_num+1
                            else:
                                need_add_list.append(cur_char)
                                need_add_num = need_add_num + 1

                except Exception as ex:
                    print(ex)
                    print("程序异常退出" )
                    break

        print("总数量:%d" % total_num + ",已经存在数量：%d" % existed_num + ",需要增加数量:%d" % need_add_num)
        for i in range(len(need_add_list)):
            self.baijiaxinglist.append(need_add_list[i])

        pythonjson = {}
        pythonjson["list"] = self.baijiaxinglist
        with open("D:\\FFOutput\\json_test.txt", 'w', encoding="utf8") as f:
            # json.dump(pythonjson, f)
            temp_result = json.dumps(pythonjson, ensure_ascii=False, indent=4)
            f.write(temp_result)

        print()
