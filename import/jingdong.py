import time


def parse_jingdong_data():

    file_full_path = r"D:\Downloads\jingdong.txt"
    insert_sql_pre = "INSERT INTO HUJI (NAME,LOGIN_NAME,EMAIL,ID_NO,PHONE,TELEPHONE) VALUES ("
    with open(file_full_path, "r", encoding="utf16") as file_handle:
        index = 0
        while True:
            try:
                current_row = file_handle.readline()
                if not current_row:
                    print("文件读取完成，共读取数据%d行" % index)
                    break
                item_list = current_row.split("---")
                index += 1
                if len(item_list) == 7:
                    sql_str = insert_sql_pre+"\""
                    sql_str = sql_str + item_list[0]
                    sql_str = sql_str + "\",\""

                    sql_str = sql_str + item_list[1]
                    sql_str = sql_str + "\",\""

                    sql_str = sql_str + item_list[3]
                    sql_str = sql_str + "\",\""

                    # 身份证号码
                    temp_id_no = item_list[4]
                    if temp_id_no == "\\N":
                        temp_id_no = ""
                    sql_str = sql_str + temp_id_no
                    sql_str = sql_str + "\",\""

                    sql_str = sql_str + item_list[5]
                    sql_str = sql_str + "\",\""

                    temp_phone = item_list[6]
                    temp_phone = temp_phone.replace("\n", "")
                    sql_str = sql_str + temp_phone
                    sql_str = sql_str + "\")"

                else:
                    print("格式不符合要求:" + current_row)

                if index % 1000000 == 0:
                    print("已经完成%d行" % index)

            except Exception as ex:
                print(ex)
                print("程序异常退出，共统计%d行" % index)
                break

        # print("文件读取完成，共读取%d行" % index)


def main():

    try:
        parse_jingdong_data()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
