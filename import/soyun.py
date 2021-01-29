import pymssql
import time
from mariadb.mariadb_manager import MariadbManager


def import_data():

    mariadb_manager = MariadbManager("192.168.1.116", 3308, "soyun", "root", "Springdawn@2016", charset="utf8mb4")
    mariadb_manager.open_connect()

    insert_sql_stmt = "INSERT INTO soyun_sgk (name, password, email, site, salt, other, id, remark)"
    insert_sql_stmt = insert_sql_stmt + "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"

    file_full_path = r"F:\soyun-script.sql"
    with open(file_full_path, "r", encoding='utf16') as file_handle:
        inserted_num = 0
        datarow_list = []
        while True:
            try:
                current_row = file_handle.readline()
                if not current_row:
                    temp_count = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    print("共计插入数据%d行" % inserted_num)
                    break
                current_row = current_row.strip()
                if len(current_row) < len("INSERT"):
                    continue
                if current_row[0:len("INSERT")].upper() == "INSERT":
                    mysql_values_tuple = build_insert_sql(current_row)
                    datarow_list.append(mysql_values_tuple)
                    if len(datarow_list) == 10000:
                        temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                        datarow_list.clear()
                        inserted_num = inserted_num + temp_num
                        print("共计插入数据%d行" % inserted_num)

            except Exception as ex:
                print(ex)
                print("已经插入数据%d行" % inserted_num)
                break


def build_insert_sql(sqlserverscript):

    mysql_values_tuple = ()
    fields_right_parenthesis_pos = sqlserverscript.find(")")

    values_str = sqlserverscript[fields_right_parenthesis_pos+1:len(sqlserverscript)]
    values_str = values_str[values_str.find("(")+1:len(values_str)-1]

    values_list = values_str.split(",")
    cur_column = 0
    for value_item in values_list:
        temp_item = value_item
        temp_item = temp_item.strip()
        cur_column = cur_column + 1

        temp_pos = temp_item.find("N'")
        temp_item_type = temp_item[0:temp_item.find("'")+1]
        if temp_item == "NULL":
            mysql_values_tuple = mysql_values_tuple + (None,)
        elif temp_pos >= 0:
            temp_item = temp_item.replace("N'", "")
            temp_item = temp_item.replace("'", "")
            temp_item = temp_item.strip()
            mysql_values_tuple = mysql_values_tuple + (temp_item,)
        elif type(temp_item).__name__ == "str":
            mysql_values_tuple = mysql_values_tuple + (temp_item,)
        else:
            print("未知的类型" + temp_item_type)
    return mysql_values_tuple


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


def main():

    try:
        import_data()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
