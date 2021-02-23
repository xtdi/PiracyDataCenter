import re
import time
import pymssql
from mariadb.mariadb_manager import MariadbManager


def import_data():

    mariadb_manager = MariadbManager("192.168.1.116", 3308, "soyun", "root", "Springdawn@2016", charset="utf8mb4")
    mariadb_manager.open_connect()

    insert_sql_stmt = "INSERT INTO soyun_sgk (name, password, email, site, salt, other, id, remark)"
    insert_sql_stmt = insert_sql_stmt + "VALUES(%s,%s,%s,%s,%s,%s,%d,%s)"

    file_full_path = r"F:\soyun-script.sql"
    with open(file_full_path, "r", encoding='utf16') as file_handle:
        inserted_num = 0
        datarow_list = []
        practical_rows_num = 0
        actual_rows_num = 0

        while True:
            try:
                current_row = file_handle.readline()
                practical_rows_num = practical_rows_num + 1
                if not current_row:
                    temp_count = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    print("共计插入数据%d行" % inserted_num)
                    break
                current_row = current_row.strip()
                if len(current_row) < len("INSERT"):
                    continue
                if current_row[0:len("INSERT")].upper() == "INSERT":

                    temp_last_str = current_row[len(current_row)-4:]
                    if temp_last_str == "N'qq":
                        next_row = file_handle.readline().strip()
                        current_row = current_row + next_row

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

    if len(values_list) != 8:
        # 字符串内容存在,号的情况
        regex = re.compile("N'.*'")
        temp_list = []
        is_special = False
        for value_item in values_list:
            temp_item = value_item.strip()
            temp_pos = temp_item.find("N'")
            if temp_pos == 0:
                match_list = regex.findall(temp_item)
                if len(match_list) == 0:
                    is_special = True
                    temp_list.append(temp_item)
                else:
                    temp_list.append(temp_item)
            else:
                if is_special:
                    cur_num = len(temp_list) - 1
                    temp_list[cur_num] = temp_list[cur_num] + "," + temp_item
                    temp_match_list = regex.findall(temp_list[cur_num])
                    if len(temp_match_list) == 0:
                        continue
                    else:
                        is_special = False
                else:
                    temp_list.append(temp_item)
        if len(temp_list) == 8:
            values_list.clear()
            values_list = temp_list
        else:
            print("----------非法的行数据："+sqlserverscript)
            return

    cur_column = 0
    for value_item in values_list:
        temp_item = value_item.strip()
        cur_column = cur_column + 1
        try:
            if cur_column == 7:
                id_value = int(temp_item)
                mysql_values_tuple = mysql_values_tuple + (id_value,)
                continue
        except Exception as ex:
            print(ex)

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


def transfer_data():

    mariadb_manager = MariadbManager("192.168.1.116", 3308, "soyun", "root", "Springdawn@2016", charset="utf8mb4")
    mariadb_manager.open_connect()

    insert_sql_stmt = "INSERT INTO soyun_sgk (name, password, email, site, salt, other, id, remark)"
    insert_sql_stmt = insert_sql_stmt + "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"

    mssql_conn = pymssql.connect(host="127.0.0.1", user="sa", password="Pmo@2016", database="new")
    mssql_cursor = mssql_conn.cursor()
    cur_row_num = 0
    inserted_num = 0
    block_len = 100000
    while True:
        try:
            query_sql = "SELECT * FROM [new].[dbo].[sgk]  where [id] > " + str(cur_row_num) + " and [id] <= "
            query_sql = query_sql + str(cur_row_num + block_len) + " order by [id] "
            mssql_cursor.execute(query_sql)
            result_row = mssql_cursor.fetchone()
            datarow_list = []
            while result_row:
                datarow_list.append(result_row)
                result_row = mssql_cursor.fetchone()

            temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
            inserted_num = inserted_num + temp_num
            datarow_list.clear()
            print("累计插入数据共计:%d行" % inserted_num)
            cur_row_num = cur_row_num + block_len

        except Exception as ex:
            print(ex)
            print("程序异常退出，已经插入数据%d行" % inserted_num)
            break


def extract_valid_qq_from_mysqlsoyun():

    soyun_manager = MariadbManager("192.168.1.116", 3308, "soyun", "root", "Springdawn@2016", charset="utf8mb4")
    soyun_manager.open_connect()

    insertdb_manager = MariadbManager("192.168.1.116", 3308, "soyun", "root", "Springdawn@2016", charset="utf8mb4")
    insertdb_manager.open_connect()

    insert_sql_stmt = "INSERT INTO temp (qq_no,phone,info_source)"
    insert_sql_stmt = insert_sql_stmt + "VALUES(%s,%s,%s)"

    mmm=0
    cur_row_num = 0
    inserted_num = 0
    block_len = 100000
    while True:
        try:
            query_sql = "SELECT name,password FROM soyun_sgk  where soyun_id > " + str(cur_row_num) + " and soyun_id <= "
            query_sql = query_sql + str(cur_row_num + block_len) + " and  site = 'qq'  and CHAR_LENGTH(`password`)=11"
            query_sql = query_sql + " and left(`password`,1)='1' order by soyun_id "
            # print(query_sql)
            soyun_query_cursor = soyun_manager.connect.cursor()
            soyun_query_cursor.execute(query_sql)
            result_row = soyun_query_cursor.fetchone()
            datarow_list = []
            while result_row:

                temp_qq_no = result_row[0]
                temp_phone = result_row[1].strip()
                temp_phone_int = 0
                try:
                    temp_phone_int = int(temp_phone)
                except Exception as intex:
                    # print(intex)
                    result_row = soyun_query_cursor.fetchone()
                    continue
                if len(str(temp_phone_int)) == 11:
                    item_tuple = (temp_qq_no, temp_phone, "soyun-qq")
                    datarow_list.append(item_tuple)
                result_row = soyun_query_cursor.fetchone()

            soyun_query_cursor.close()
            temp_num = batch_insert_data(insertdb_manager.connect, insert_sql_stmt, datarow_list)
            inserted_num = inserted_num + temp_num
            datarow_list.clear()
            print("累计插入数据共计:%d行" % cur_row_num)
            cur_row_num = cur_row_num + block_len

        except Exception as ex:
            print(ex)
            print("程序异常退出，已经插入数据%d行" % inserted_num)
            break


def main():

    try:
        # transfer_data()
        extract_valid_qq_from_mysqlsoyun()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
