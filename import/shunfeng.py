import time
from mariadb.mariadb_manager import MariadbManager


# 生产类似于（value1,value2,value3）的values值字符串
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

    mariadb_manager = MariadbManager("192.168.232.128", 3306, "privacydata", "root", "pmo@2016",  charset='GBK')
    mariadb_manager.connect()

    insert_sql_stmt = "INSERT INTO shunfeng (name, phone, province, city, dist, addr)VALUES(%s,%s,%s,%s,%s,%s)"

    file_full_path = r"D:\downloads\privacydata\shunfeng.sql"
    with open(file_full_path, "r", encoding='utf16') as file_handle:
        index = 0
        inserted_num = 0
        datarow_list = []
        while True:
            try:
                current_row = file_handle.readline()
                if not current_row:
                    temp_count = insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    print("已经插入数据%d行" % inserted_num)
                    break
                current_row = current_row.strip()
                if len(current_row) < len("INSERT"):
                    continue
                if current_row[0:len("INSERT")].upper() == "INSERT":
                    # mysql_sql_str = convert_insertsql(current_row)
                    mysql_values_tuple = build_insert_values(current_row)
                    datarow_list.append(mysql_values_tuple)
                    if len(datarow_list) == 10000:
                        insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                        datarow_list.clear()
                        inserted_num = inserted_num + 10000
                        print("已经插入数据%d行" % inserted_num)
            except Exception as ex:
                print(ex)
                print("已经插入数据%d行" % inserted_num)
                break


def insert_data(mariadb_conn, sql_stmt, values_list):

    begin_time = time.time()
    table_cursor = mariadb_conn.cursor()
    total_count = table_cursor.executemany(sql_stmt, values_list)
    mariadb_conn.commit()
    table_cursor.close()
    end_time = time.time()
    print("共计插入%d条"+ total_count+"  共计花费时间:" + str(end_time-begin_time))
    return total_count

def main():

    try:
        temp=[(1,2,3),(3,4,5)]

        parse_sqlserverscript()
        # convert_insertsql("INSERT [dbo].[Big_1_shunfeng] ([name], [phone], [province], [city], [dist], [addr]) VALUES (N'', N'0000000', N'四川省', N'南充市', N'顺庆区', N'四川省南充市顺庆区新政世纪明珠小区15幢2单元')")
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
