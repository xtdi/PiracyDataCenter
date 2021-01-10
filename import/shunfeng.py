import time
from mariadb.mariadb_manager import MariadbManager


# 生成类似于（value1,value2,value3）的values值字符串
def build_insert_values(sqlserverscript):

    mysql_values_tuple = ()
    fields_right_parenthesis_pos = sqlserverscript.find(")")

    values_str = sqlserverscript[fields_right_parenthesis_pos+1:len(sqlserverscript)]
    values_str = values_str[values_str.find("(")+1:len(values_str)-1]

    values_list = values_str.split(",")
    for value_item in values_list:
        temp_item = value_item
        temp_item = temp_item.strip()
        count_num = temp_item.count("'")

        temp_item_type = temp_item[0:temp_item.find("'")]
        if temp_item_type.upper() == 'N':
            temp_item = temp_item.replace("'", "")
            temp_item = temp_item.strip()
            mysql_values_tuple = mysql_values_tuple + (temp_item[1:len(temp_item)],)
        else:
            print(temp_item_type)

    # mysql_values_str = mysql_values_str[0:len(mysql_values_str) - 1] + ")"
    # print(mysql_sql_str)
    return mysql_values_tuple


def convert_insertsql(sqlserverscript):

    mysql_sql_str = "INSERT INTO shunfeng "
    sqlserverscript = sqlserverscript.replace("[dbo].", "")

    fields_left_parenthesis_pos = sqlserverscript.find("(")
    fields_right_parenthesis_pos = sqlserverscript.find(")")

    """
    temp_insert_str = sqlserverscript[0:fields_left_parenthesis_pos-1].strip()
    table_name = temp_insert_str[temp_insert_str.rfind(" ")+1:len(temp_insert_str)]
    table_name = tabel_name.replace("[", "")
    table_name = tabel_name.replace("]", "")
    mysql_sql_str = mysql_sql_str + table_name
    """
    fileds_words = sqlserverscript[fields_left_parenthesis_pos:fields_right_parenthesis_pos+1]
    fileds_words = fileds_words.replace("[", "")
    fileds_words = fileds_words.replace("]", "")
    mysql_sql_str = mysql_sql_str + fileds_words
    mysql_sql_str = mysql_sql_str + "VALUES"

    values_str = sqlserverscript[fields_right_parenthesis_pos+1:len(sqlserverscript)]
    values_str = values_str[values_str.find("(")+1:len(values_str)-1]

    mysql_values_str = ""
    values_list = values_str.split(",")
    for value_item in values_list:
        temp_item = value_item
        temp_item = temp_item.strip()
        temp_item_type = temp_item[0:temp_item.find("'")]
        if temp_item_type.upper() == 'N':
            print(temp_item_type)
            temp_item = temp_item.replace("'", "\"")
            if temp_item == "15264782588":
                print()
            mysql_values_str = mysql_values_str + temp_item[1:len(temp_item)] + ","
        elif temp_item_type.upper() == "INSERT":
            print(temp_item_type)
        else:
            print(temp_item_type)

    mysql_sql_str = mysql_sql_str + "(" + mysql_values_str[0:len(mysql_values_str) - 1] + ""
    # print(mysql_sql_str)
    return mysql_sql_str


def parse_sqlserverscript():

    # mariadb_manager = MariadbManager("192.168.232.128", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager.open_connect()

    insert_sql_stmt = "INSERT INTO shunfeng (name, phone, province, city, dist, addr)VALUES(%s,%s,%s,%s,%s,%s)"

    file_full_path = r"D:\downloads\privacydata\shunfeng.sql"
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
                    # mysql_sql_str = convert_insertsql(current_row)
                    mysql_values_tuple = build_insert_values(current_row)
                    datarow_list.append(mysql_values_tuple)
                    if len(datarow_list) == 10000:
                        temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                        # insert_data(mariadb_manager, datarow_list)
                        datarow_list.clear()
                        inserted_num = inserted_num + temp_num
                        print("共计插入数据%d行" % inserted_num)
            except Exception as ex:
                print(ex)
                print("已经插入数据%d行" % inserted_num)
                break


def batch_insert_data(mariadb_conn, sql_stmt, values_list):

    begin_time = time.time()
    table_cursor = mariadb_conn.cursor()
    total_count = 0
    try:
        total_count = table_cursor.executemany(sql_stmt, values_list)
        mariadb_conn.commit()
    except Exception as ex:
        print(ex)
        tempname = values_list[7211][0]
        for j in tempname:
            print(j)
        mariadb_conn.rollback()
    table_cursor.close()
    end_time = time.time()
    spend_time = end_time-begin_time

    print("本次插入数据%d条" % total_count+"  共计花费时间:%s秒" % format(spend_time, '0.2f'))
    return total_count


def insert_data(mariadb_manager, values_list):

    mysql_sql_prefix = "INSERT INTO shunfeng (name, phone, province, city, dist, addr)VALUES"
    for i in range(len(values_list)):
        row_value = values_list[i]
        mysql_values_str = ""
        for item_value in row_value:
            mysql_values_str = mysql_values_str + "'"
            mysql_values_str = mysql_values_str + item_value
            mysql_values_str = mysql_values_str + "',"

        temp_sql = mysql_sql_prefix + "(" + mysql_values_str[0:len(mysql_values_str) - 1] + ")"
        print(temp_sql)
        mariadb_manager.insert(temp_sql)


def main():

    try:
        # parse_sqlserverscript()
        print()
        # convert_insertsql("INSERT [dbo].[Big_1_shunfeng] ([name], [phone], [province], [city], [dist], [addr]) VALUES (N'', N'0000000', N'四川省', N'南充市', N'顺庆区', N'四川省南充市顺庆区新政世纪明珠小区15幢2单元')")
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
