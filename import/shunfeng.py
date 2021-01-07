import time
from mariadb.mariadb_manager import MariadbManager

def convert_insertsql(sqlserverscript):

    mysql_sql_str = "INSERT INTO shunfeng "
    sqlserverscript = sqlserverscript.replace("[dbo].", "")

    fields_left_parenthesis_pos = sqlserverscript.find("(")
    fields_right_parenthesis_pos = sqlserverscript.find(")")

    """"
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
            mysql_values_str = mysql_values_str + temp_item[1:len(temp_item)] + ","
        elif temp_item_type.upper() == "INSERT":
            print(temp_item_type)
        else:
            print(temp_item_type)

    mysql_sql_str = mysql_sql_str + "(" + mysql_values_str[0:len(mysql_values_str) - 1] + ")"
    # print(mysql_sql_str)
    return mysql_sql_str


def parse_sqlserverscript():

    mariadbManager = MariadbManager("192.168.232.128", 3306, "privacydata", "root", "pmo@2016",  charset='GBK')
    mariadbManager.connect()

    file_full_path = r"D:\downloads\privacydata\shunfeng.sql"
    with open(file_full_path, "r", encoding='utf16') as file_handle:
        index = 0
        num = 0
        while True:
            try:
                current_row = file_handle.readline()

                if not current_row:
                    print("文件读取完成，共读取数据%d行" % index)
                    break
                current_row = current_row.strip()
                if len(current_row) < len("INSERT"):
                    continue
                if current_row[0:len("INSERT")].upper() == "INSERT":
                    mysql_sql_str = convert_insertsql(current_row)
                    index += 1
                    if index % 1000 == 0:
                        print("已经完成%d行" % index)
            except Exception as ex:
                print(ex)
                print("共统计%d行" % index)
                print("---------%d行" % num)
                break


def main():

    try:
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
