import time
from mariadb.mariadb_manager import MariadbManager


def import_weibo_data():

    mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager.open_connect()
    insert_sql_stmt = "INSERT INTO weibo (phone, uid)VALUES(%s,%s)"

    file_full_path = r"D:\Downloads\privacydata\weibo2019.txt"
    with open(file_full_path, "r", encoding='utf-8') as file_handle:
        inserted_num = 0
        datarow_list = []
        total_rows = 0
        current_row = file_handle.readline()
        while True:
            try:
                total_rows = total_rows + 1
                current_row = file_handle.readline()
                if not current_row:
                    temp_count = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    print("共计插入数据%d行" % inserted_num)
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
                    if len(tel_str) > 0 and len(uid_str) > 0:
                        mysql_values_tuple = (tel_str, uid_str)
                        datarow_list.append(mysql_values_tuple)
                        if len(datarow_list) == 10000:
                            temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                            inserted_num = inserted_num + temp_num
                            datarow_list.clear()
                            print("共计插入数据%d行" % inserted_num)

            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % inserted_num)
                break
        print("文件读取完成，物理行共计%d行" % total_rows + " 共插入数据%d行" % inserted_num)


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


def parse_weibo_data():

    file_full_path = r"D:\Downloads\privacydata\weibo2019.txt"
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
        import_weibo_data()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
