import time
from mariadb.mariadb_manager import MariadbManager

# 邢涛---richard---8e5ea577c79a36cd8247a132834c7662022755dd---xbrc@163.com---510726197604054039---13591610030---0410-6120166


def parse_jingdong_data():

    mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager.open_connect()

    file_full_path = r"D:\Downloads\privacydata\jingdong.txt"
    insert_sql_stmt = "INSERT INTO jingdong (NAME,LOGIN_NAME,EMAIL,ID_NO,PHONE,TELEPHONE) VALUES(%s,%s,%s,%s,%s,%s)"

    with open(file_full_path, "r", encoding="utf8") as file_handle:
        total_rows = 0
        inserted_num = 0
        datarow_list = []
        invalid_format_rows_list = []
        error_num = 0
        while True:
            try:
                total_rows = total_rows + 1
                current_row = file_handle.readline()
                if not current_row:
                    print("文件读取完成，共读取数据%d行" % total_rows)
                    break
                item_list = current_row.split("---")

                temp_name = item_list[0].strip()  # 姓名

                """
                temp_login_name = ""
                temp_email = ""
                password_pos = -1
                email_pos = -1
                for cur_pos in range(len(item_list)):
                    temp_cur_value = item_list[cur_pos].strip()
                    if len(temp_cur_value) == 40:
                        password_pos = cur_pos
                    if temp_cur_value.find("@") >= 0:
                        email_pos = cur_pos
                if password_pos > 0:
                    for i in range(1, password_pos, 1):
                        temp_login_name = temp_login_name + item_list[i].strip()
                if email_pos > 0:
                    for i in range(password_pos+1, email_pos+1, 1):
                        temp_email = temp_email + item_list[i].strip()

                    temp_id_no = item_list[email_pos+1].strip()       # 身份证号码
                    temp_phone = item_list[email_pos+2].strip()       # 手机号码
                    temp_telephone = item_list[email_pos+3].strip()   # 电话号码

                else:
                    print("")

                """
                if len(item_list) == 7:
                    temp_login_name = item_list[1].strip()  # 登录用户
                    temp_email = item_list[3].strip()       # 电子邮件
                    temp_id_no = item_list[4].strip()       # 身份证号码
                    temp_phone = item_list[5].strip()       # 手机号码
                    temp_telephone = item_list[6].strip()   # 电话号码

                    if temp_id_no.find("\\N") >= 0:
                        temp_id_no = temp_id_no.replace("\\N", "")

                    mysql_values_tuple = (temp_name, temp_login_name, temp_email, temp_id_no, temp_phone, temp_telephone)
                    datarow_list.append(mysql_values_tuple)
                else:
                    error_num = error_num + 1
                    invalid_format_rows_list.append(current_row)
                    # print("格式不符合要求:" + current_row)

                if total_rows % 1000000 == 0:
                    """
                    temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_num
                    datarow_list.clear()
                    print("共计插入数据%d行" % inserted_num)
                    """
                    print("共计插入数据" + str(total_rows) + "行， 格式错误行："+  str(error_num))

            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % inserted_num)
                break
        print(error_num)

        # print("文件读取完成，共读取%d行" % index)

def batch_insert_data(mariadb_conn, sql_stmt, values_list):

    return len(values_list)

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
        parse_jingdong_data()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
