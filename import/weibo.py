import time


def parse_weibo_data():

    file_full_path = r"D:\Downloads\weibo2019.txt"
    with open(file_full_path, "r", encoding='utf-8') as file_handle:
        index = 0
        current_row = file_handle.readline()
        while True:
            try:
                current_row = file_handle.readline()
                if not current_row:
                    print("文件读取完成，共读取数据%d行" % index)
                    break
                current_row = current_row.strip()
                if len(current_row) == 0:
                    continue
                first_blank_pos = current_row.find(" ")
                if first_blank_pos == -1:
                    first_blank_pos = current_row.find("\t")

                if first_blank_pos > 0:
                    tel_str = current_row[0:first_blank_pos].strip()
                    uid_str = current_row[first_blank_pos:len(current_row)].strip()
                    index += 1
                    if len(tel_str) > 0 and len(uid_str) > 0:
                        mysql_sql_str = "INSERT INTO WEIBO "
                        mysql_sql_str = mysql_sql_str + "(PHONE , UID) VALUES (\""
                        mysql_sql_str = mysql_sql_str + tel_str
                        mysql_sql_str = mysql_sql_str + "\"，\""
                        mysql_sql_str = mysql_sql_str + uid_str
                        mysql_sql_str = mysql_sql_str + "\")"

                        if index % 1000000 == 0:
                            print("已经完成%d行" % index)

            except Exception as ex:
                print(ex)
                print("程序退出，共统计%d行" % index)
                break

        # print("文件读取完成，共读取%d行" % index)


def main():

    # read_sql('res_error.sql', '11.sql')

    try:
        parse_weibo_data()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
