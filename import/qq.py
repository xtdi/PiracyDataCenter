import time
from mariadb.mariadb_manager import MariadbManager

def parse_data():

    mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager.open_connect()
    insert_sql_stmt = "INSERT INTO qq (phone, uid)VALUES(%s,%s)"

    file_full_path = r"D:\downloads\privacydata\qq6.9.txt"
    with open(file_full_path, "r") as file_handle:
        inserted_num = 0
        datarow_list = []
        total_rows = 0
        sameqq_item_list = []
        invalid_rows_num = 0
        invalid_rows_list = []
        row_content_null_num = 0
        while True:
            try:
                total_rows = total_rows + 1
                if total_rows % 1000000 == 0:
                    print("已经读取行数%d" % total_rows)

                current_row = file_handle.readline()
                if not current_row:
                    temp_count = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    print("共计插入数据%d行" % inserted_num)
                    print("无效数据%d行" % invalid_rows_num)
                    print("三项数据中QQ数据相等的数据记录数：行"% len(sameqq_item_list))
                    with open(r"D:\qq_invalid_rows.txt", "w", encoding="utf8") as f:
                        f.writelines(invalid_rows_list)
                        invalid_rows_list.clear()
                    with open(r"D:\qq_sameqq_rows.txt", "w", encoding="utf8") as f:
                        f.writelines(sameqq_item_list)
                        sameqq_item_list.clear()
                    break

                current_row = current_row.strip()
                if len(current_row) == 0:
                    invalid_rows_num = invalid_rows_num + 1
                    row_content_null_num = row_content_null_num + 1
                    invalid_rows_list.append(current_row + "\n")
                    continue

                item_list = current_row.split("----")

                if len(item_list) == 2:
                    mysql_values_tuple = (item_list[1], item_list[0])
                    datarow_list.append(mysql_values_tuple)
                elif len(item_list) == 3:
                    if item_list[0].strip() == item_list[1].strip():
                        mysql_values_tuple = (item_list[2], item_list[0])
                        datarow_list.append(mysql_values_tuple)
                        sameqq_item_list.append(current_row + "\n")
                    else:
                        invalid_rows_num = invalid_rows_num + 1
                        invalid_rows_list.append(current_row + "\n")
                        continue
                else:
                    invalid_rows_num = invalid_rows_num + 1
                    invalid_rows_list.append(current_row + "\n")
                    continue

                if len(datarow_list) == 100000:
                    temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_num
                    datarow_list.clear()
                    # print("共计插入数据%d行" % inserted_num)

            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % inserted_num)
                break
        print("文件读取完成，物理行共计%d行" % (total_rows-1) + " 共插入数据%d行" % inserted_num)
        print("空行数为:%d" % row_content_null_num)


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


def parse_multiqq_data():

    mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager.open_connect()
    insert_sql_stmt = "INSERT INTO qq (phone, uid, has_multi_qq)VALUES(%s,%s,%s)"

    file_full_path = r"D:\qq_invalid_rows.txt"
    with open(file_full_path, "r") as file_handle:
        inserted_num = 0
        datarow_list = []
        total_rows = 0
        invalid_rows_num = 0
        invalid_rows_list = []
        while True:
            try:
                total_rows = total_rows + 1
                if total_rows % 10000 == 0:
                    print("已经读取行数%d" % total_rows)

                current_row = file_handle.readline()
                if not current_row:
                    temp_count = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    print("共计插入数据%d行" % inserted_num)
                    print("无效数据%d行" % invalid_rows_num)
                    with open(r"D:\qq_other_rows.txt", "w", encoding="utf8") as f:
                        f.writelines(invalid_rows_list)
                        invalid_rows_list.clear()
                    break

                current_row = current_row.strip()
                if len(current_row) == 0:
                    invalid_rows_num = invalid_rows_num + 1
                    invalid_rows_list.append(current_row + "(row-null)\n")
                    continue

                item_list = current_row.split("----")

                if len(item_list) == 3:
                    first_mysql_values_tuple = (item_list[2], item_list[0], 1)
                    second_mysql_values_tuple = (item_list[2], item_list[1], 1)
                    datarow_list.append(first_mysql_values_tuple)
                    datarow_list.append(second_mysql_values_tuple)
                else:
                    invalid_rows_num = invalid_rows_num + 1
                    invalid_rows_list.append(current_row + "\n")
                    continue

                if len(datarow_list) == 10000:
                    temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_num
                    datarow_list.clear()

            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % inserted_num)
                break
        print("文件读取完成，物理行共计%d行" % (total_rows-1) + " 共插入数据%d行" % inserted_num)


def main():

    try:
        # parse_data()
        parse_multiqq_data()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)

