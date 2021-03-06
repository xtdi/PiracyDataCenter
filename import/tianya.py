import time
import os
from mariadb.mariadb_manager import MariadbManager


def parse_each_txt(txtfile_full_path, mariadb_conn, insert_sql_stmt):

    invalid_records_file_path = r"D:\tianya_invalid_format_records.txt"

    with open(txtfile_full_path, "r", encoding="utf8") as file_handle:
        total_rows = 0
        inserted_num = 0
        datarow_list = []
        blank_rows_num = 0
        invalid_format_rows_list = []
        invalid_format_rows_num = 0
        while True:
            try:
                total_rows = total_rows + 1

                if len(invalid_format_rows_list) == 10000:
                    with open(invalid_records_file_path, "a", "utf8") as f:
                        f.writelines(invalid_format_rows_list)
                        invalid_format_rows_list.clear()

                current_row = file_handle.readline()
                if not current_row:
                    temp_count = batch_insert_data(mariadb_conn, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    datarow_list.clear()
                    # print("文件读取完成，共计读取数据%d行" % (total_rows-1)  + ",其中：插入数据：%d行" % inserted_num)
                    # print("格式无效数据:%d行" % invalid_format_rows_num)
                    # print("空数据:%d行" % blank_rows_num)

                    with open(invalid_records_file_path, "a", encoding="utf8") as f:
                        f.write("-----" + txtfile_full_path + "\n")
                        f.writelines(invalid_format_rows_list)
                        invalid_format_rows_list.clear()
                    break

                current_row = current_row.strip()
                if len(current_row) == 0:
                    blank_rows_num = blank_rows_num + 1
                    continue

                item_list = []
                split_char = ""
                first_blank_pos = current_row.find(" ")
                first_douhao_pos = current_row.find(",")
                if first_blank_pos > 0:
                    if current_row.count(",") == 2:
                        split_char = ","
                    else:
                        split_char = " "
                else:
                    if first_douhao_pos > 0:
                        split_char = ","
                    else:
                        print("(" + str(total_rows) + ") 行无法确认分隔符：" + current_row)
                        invalid_format_rows_num = invalid_format_rows_num + 1
                        invalid_format_rows_list.append(current_row + "\n")
                        continue
                temp_all_list = current_row.split(split_char)
                for i in range(len(temp_all_list)):
                    if len(temp_all_list[i].strip()) > 0:
                        item_list.append(temp_all_list[i].strip())
                if len(item_list) != 3:
                    # print("数据项不足三项："+current_row)
                    invalid_format_rows_num = invalid_format_rows_num + 1
                    invalid_format_rows_list.append(current_row + "\n")
                    continue

                temp_login_name = item_list[0].strip()
                temp_password = item_list[1].strip()
                temp_email = item_list[2].strip()

                if temp_email.find("@") == -1:
                    invalid_format_rows_num = invalid_format_rows_num + 1
                    invalid_format_rows_list.append(current_row + "\n")
                    continue

                if temp_email.find("<") >= 0 and temp_email.find(">") > 0:
                    temp_first_pos = temp_email.find("<")
                    temp_two_pos = temp_email.find(">")
                    if temp_first_pos > 0:
                        print(temp_email)
                    temp_email = temp_email[temp_first_pos+1:temp_two_pos-temp_first_pos]

                if temp_email.find("<") >= 0:
                    temp_email = temp_email.replace("<", "")
                if temp_email.find(">") >= 0:
                    temp_email = temp_email.replace(">", "")
                if temp_email.find("；") >= 0:
                    temp_email = temp_email.replace("；", "")
                if temp_email.find("意见反馈") >= 0 or temp_email.find("帮助中心") >= 0 or temp_email.find("退出") >= 0:
                    print(item_list[2].strip())

                mysql_values_tuple = (temp_login_name, temp_password, temp_email)
                datarow_list.append(mysql_values_tuple)
                if len(datarow_list) == 10000:


                    too_long_str = ""
                    temp_max_long_len = 0
                    temp_max_long_uid = ""
                    for num in range(len(datarow_list)):
                        cur_temp_uid = datarow_list[num][0]
                        cur_temp_phone_len = len(datarow_list[num][2])
                        if cur_temp_phone_len > temp_max_long_len:
                            temp_max_long_len = cur_temp_phone_len
                            temp_max_long_uid = cur_temp_uid
                            too_long_str = "----------------------------------------" + temp_max_long_uid + ":" + str(temp_max_long_len)
                    print("开始本批插入，本批第一项数据为:" + datarow_list[0][1] + "-" + datarow_list[0][0] + "." + too_long_str)


                    temp_num = batch_insert_data(mariadb_conn, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_num
                    datarow_list.clear()
                    # print("共计插入数据%d行" % inserted_num)
            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % inserted_num)
                break

    return_result_str = "文件共计:" + str(total_rows - 1) + "行，其中插入数据:" + str(inserted_num)
    return_result_str = return_result_str + "行，格式无效数据:" + str(invalid_format_rows_num)
    return_result_str = return_result_str + "行，空数据:" + str(blank_rows_num) + "行"
    return return_result_str


def batch_insert_data(mariadb_conn, sql_stmt, values_list):

    begin_time = time.time()
    table_cursor = mariadb_conn.cursor()
    total_count = 0
    try:
        total_count = table_cursor.executemany(sql_stmt, values_list)
        mariadb_conn.commit()
    except Exception as ex:
        print(ex)
        mariadb_conn.rollback()
    table_cursor.close()
    end_time = time.time()
    spend_time = end_time-begin_time
    print("本次插入数据%d条" % total_count+"  共计花费时间:%s秒" % format(spend_time, '0.2f'))
    return total_count


def parse_all_files():

    invalid_records_file_path = r"D:\tianya_invalid_format_records.txt"
    with open(invalid_records_file_path, "w", encoding="utf-8") as file:
        print("初始化格式不规范文件")

    tianya_log_file_path = r"D:\tianya_log.txt"
    with open(tianya_log_file_path, "w", encoding="utf-8") as logfile:
        print("初始化日志文件")

    # mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager = MariadbManager("192.168.1.116", 3308, "privacydata", "root", "Springdawn@2016", charset="utf8mb4")
    mariadb_manager.open_connect()
    insert_sql_stmt = "INSERT INTO tianya (login_name, password, email)"
    insert_sql_stmt = insert_sql_stmt + " VALUES (%s, %s, %s)"
    rootdir = r"D:\downloads\privacydata\tianya-new"
    list = os.listdir(rootdir)  # 列出文件夹下所有的目录与文件
    file_num = 0
    for i in range(0, len(list)):
        file_path = os.path.join(rootdir, list[i])
        if os.path.isfile(file_path):
            file_num = file_num + 1
            print("开始第" + str(file_num) + "文件:" + file_path + "！")
            # if file_path == r"D:\downloads\privacydata\huji\1 - 副本 (18).xlsx":
            finised_info = parse_each_txt(file_path, mariadb_manager.connect, insert_sql_stmt)
            with open(tianya_log_file_path, "a", encoding="utf-8") as logfile:
                write_info = "完成第%d个文件（" % file_num + file_path + ")的数据插入，完成信息:" + finised_info + " \n"
                print("完成第%d个文件（" % file_num + file_path + ")的数据插入，完成信息:" + finised_info + " \n")
                logfile.write(write_info + "\n" )


def parse_invalid_records():

    # mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager = MariadbManager("192.168.1.116", 3308, "privacydata", "root", "Springdawn@2016", charset="utf8mb4")
    mariadb_manager.open_connect()
    insert_sql_stmt = "INSERT INTO tianya (login_name, password, email)"
    insert_sql_stmt = insert_sql_stmt + " VALUES (%s, %s, %s)"

    records_file_path = r"D:\tianya_invalid_format-dxt.txt"
    invalid_records_file_path = r"D:\tianya_new_invalid.txt"
    with open(records_file_path, "r", encoding="utf8") as file_handle:
        total_rows = 0
        inserted_num = 0
        datarow_list = []
        blank_rows_num = 0
        invalid_format_rows_list = []
        invalid_format_rows_num = 0
        while True:
            try:
                total_rows = total_rows + 1

                current_row = file_handle.readline()
                if not current_row:
                    temp_count = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    datarow_list.clear()
                    # print("文件读取完成，共计读取数据%d行" % (total_rows-1)  + ",其中：插入数据：%d行" % inserted_num)
                    # print("格式无效数据:%d行" % invalid_format_rows_num)
                    # print("空数据:%d行" % blank_rows_num)

                    with open(invalid_records_file_path, "w", encoding="utf8") as f:
                        f.writelines(invalid_format_rows_list)
                        invalid_format_rows_list.clear()
                    break

                current_row = current_row.strip()
                if len(current_row) == 0:
                    blank_rows_num = blank_rows_num + 1
                    continue

                item_list = []
                split_char = " "
                temp_all_list = current_row.split(split_char)
                for i in range(len(temp_all_list)):
                    if len(temp_all_list[i].strip()) > 0:
                        item_list.append(temp_all_list[i].strip())
                if len(item_list) < 2:
                    # print("数据项不足三项："+current_row)
                    invalid_format_rows_num = invalid_format_rows_num + 1
                    invalid_format_rows_list.append(current_row + "\n")
                    continue

                temp_login_name = item_list[0].strip()
                temp_password = ""
                temp_email = item_list[len(item_list)-1].strip()

                if temp_email.find("@") == -1:
                    invalid_format_rows_num = invalid_format_rows_num + 1
                    invalid_format_rows_list.append(current_row + "\n")
                    continue

                for i in range(len(item_list)):
                    if i == 0 or i == len(item_list) - 1:
                        continue
                    else:
                        temp_password = temp_password + item_list[i]

                temp_password = temp_password.replace(" ","")
                mysql_values_tuple = (temp_login_name, temp_password, temp_email)
                datarow_list.append(mysql_values_tuple)
                if len(datarow_list) == 10000:
                    temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_num
                    datarow_list.clear()
                    # print("共计插入数据%d行" % inserted_num)
            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % inserted_num)
                break

    return_result_str = "文件共计:" + str(total_rows - 1) + "行，其中插入数据:" + str(inserted_num)
    return_result_str = return_result_str + "行，格式无效数据:" + str(invalid_format_rows_num)
    return_result_str = return_result_str + "行，空数据:" + str(blank_rows_num) + "行"
    return return_result_str

def main():

    try:
        parse_invalid_records()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)

# xtdi 0118

