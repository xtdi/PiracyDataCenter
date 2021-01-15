import time
import os
from mariadb.mariadb_manager import MariadbManager


def parse_each_txt(txtfile_full_path, mariadb_conn, insert_sql_stmt):

    logfile_path = r"D:\tianya-log.txt"

    with open(txtfile_full_path, "r", encoding="utf8") as file_handle:
        total_rows = 0
        inserted_num = 0
        datarow_list = []
        invalid_format_rows_list = []
        invalid_format_rows_num = 0
        while True:
            try:
                total_rows = total_rows + 1

                if len(invalid_format_rows_list) == 10000:
                    with open("D:\invalid_format_records.txt", "a") as f:
                        f.writelines(invalid_format_rows_list)
                        invalid_format_rows_list.clear()

                current_row = file_handle.readline()
                if not current_row:
                    temp_count = batch_insert_data(mariadb_conn, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    datarow_list.clear()
                    print("文件读取完成，共计插入数据%d行" % inserted_num)

                    with open("D:\invalid_format_records.txt", "a") as f:
                        f.writelines(invalid_format_rows_list)
                        invalid_format_rows_list.clear()
                    break

                temp_login_name = ""
                temp_password = ""
                temp_email = ""

                mysql_values_tuple = (temp_login_name, temp_password, temp_email)
                datarow_list.append(mysql_values_tuple)
                if len(datarow_list) == 10000:
                    temp_num = batch_insert_data(mariadb_conn, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_num
                    datarow_list.clear()
                    print("共计插入数据%d行" % inserted_num)
            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % total_rows)
                break

    with open(logfile_path, "w") as file:
        file.write(logfile_path + "  文件入库完成，共入库数据"+str(inserted_num)+"行,因电话号码空没有插入的数据条数为：" + "\n")


def batch_insert_data(mariadb_conn, sql_stmt, values_list):

    # return len(values_list)

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

    logfile_path = r"D:\tianya-log.txt"
    with open(logfile_path, "w", encoding="utf-8") as file:
        print("初始化日志文件")

    mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager.open_connect()
    insert_sql_stmt = "INSERT INTO tianya (login_name, password, email)"
    insert_sql_stmt = insert_sql_stmt + " VALUES (%s, %s, %s)"
    rootdir = r"D:\downloads\privacydata\tianya"
    list = os.listdir(rootdir)  # 列出文件夹下所有的目录与文件
    file_num = 0
    for i in range(0, len(list)):
        file_path = os.path.join(rootdir, list[i])
        if os.path.isfile(file_path):
            file_num = file_num + 1
            print("开始第" + str(file_num) + "文件:" + file_path + "！")
            # if file_path == r"D:\downloads\privacydata\huji\1 - 副本 (18).xlsx":

            # parse_huji_excel(file_path, mariadb_manager.connect, insert_sql_stmt)


def main():

    try:
        parse_all_files()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
