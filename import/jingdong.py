import time
from mariadb.mariadb_manager import MariadbManager
from utils.baijiaxing import Baijiaxing


def parse_jingdong_data():

    mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager.open_connect()

    with open("D:\invalid_format_records.txt", "w") as f:
        f.write("")

    file_full_path = r"D:\Downloads\privacydata\jingdong.txt"
    insert_sql_stmt = "INSERT INTO jingdong (NAME,LOGIN_NAME,EMAIL,ID_NO,PHONE,TELEPHONE) VALUES(%s,%s,%s,%s,%s,%s)"

    with open(file_full_path, "r", encoding="utf8") as file_handle:
        total_rows = 0
        inserted_num = 0
        datarow_list = []
        invalid_format_rows_list = []
        invalid_format_rows_num = 0
        while True:
            try:
                total_rows = total_rows + 1

                if len(invalid_format_rows_list) == 1000000:
                    with open("D:\invalid_format_records.txt", "a") as f:
                        f.writelines(invalid_format_rows_list)
                        invalid_format_rows_list.clear()

                current_row = file_handle.readline()
                if not current_row:
                    temp_count = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    datarow_list.clear()
                    print("文件读取完成，共计插入数据%d行" % inserted_num)

                    with open("D:\invalid_format_records.txt", "a") as f:
                        f.writelines(invalid_format_rows_list)
                        invalid_format_rows_list.clear()
                    break

                current_row = current_row.replace("\\N", "")
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

                    if temp_email.find("@") == -1:
                        invalid_format_rows_num = invalid_format_rows_num + 1
                        invalid_format_rows_list.append(current_row)
                        continue

                    mysql_values_tuple = (temp_name, temp_login_name, temp_email, temp_id_no, temp_phone, temp_telephone)
                    datarow_list.append(mysql_values_tuple)
                    if len(datarow_list) == 100000:
                        temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                        inserted_num = inserted_num + temp_num
                        datarow_list.clear()
                        print("共计插入数据" + str(inserted_num) + "行， 格式错误行：" + str(invalid_format_rows_num))
                else:
                    invalid_format_rows_num = invalid_format_rows_num + 1
                    invalid_format_rows_list.append(current_row)
                    continue
                    # print("格式不符合要求:" + current_row)

            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % inserted_num)
                break

        if len(invalid_format_rows_list) > 0:
            with open("D:\invalid_format_records.txt", "a") as f:
                f.writelines(invalid_format_rows_list)

        print("文件总条数：" + str(total_rows) + ",共计插入条数："
              + str(inserted_num) + " 格式不符合要求条数：" + str(invalid_format_rows_num))


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


def handle_error_records():

    file_full_path = r"D:\FFOutput\invalid_format_records.txt"
    with open(file_full_path, "r") as file_handle:
        total_rows = 0
        other_list = []
        no_name_list = []
        no_name_rows_num = 0
        other_rows_num = 0
        while True:
            try:
                total_rows = total_rows + 1
                if total_rows % 10000 == 0:
                    print("已经读取%d行"% total_rows)

                current_row = file_handle.readline()
                if not current_row:
                    print("文件读取完成，共读取数据%d行" % (total_rows-1) + ",无名字记录%d行"% no_name_rows_num
                          + ", 其他%d行" % other_rows_num)
                    with open("D:/FFOutput/no_name.txt", "w") as no_name_file:
                        no_name_file.writelines(no_name_list)
                    with open("D:/FFOutput/other.txt", "w") as f:
                        f.writelines(other_list)
                    break

                current_row = current_row.replace("\\N", "")
                item_list = current_row.split("---")
                temp_name = item_list[0].strip()  # 姓名
                if len(temp_name) == 0:
                    no_name_list.append(current_row)
                    no_name_rows_num = no_name_rows_num + 1
                else:
                    other_list.append(current_row)
                    other_rows_num = other_rows_num + 1

            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % total_rows)
                break

def handle_chinesename_records():

    file_full_path = r"D:\FFOutput\other.txt"
    with open(file_full_path, "r") as file_handle:
        total_rows = 0
        other_list = []
        all_chinese_name_list = []
        all_chinese_name_rows_num = 0
        other_rows_num = 0
        while True:
            try:
                total_rows = total_rows + 1
                if total_rows % 10000 == 0:
                    print("已经读取%d行"% total_rows)

                current_row = file_handle.readline()
                if not current_row:
                    print("文件读取完成，共读取数据%d行" % (total_rows-1) + ",名字全中文记录%d行"% all_chinese_name_rows_num
                          + ", 其他%d行" % other_rows_num)
                    print("all_chinese_name_list:据%d行" % len(all_chinese_name_list)
                          + ",other_list:%d行" % len(other_list))
                    with open("D:/FFOutput/all_chinese_name.txt", "w") as all_chinese_name_file:
                        all_chinese_name_file.writelines(all_chinese_name_list)
                    with open("D:/FFOutput/yuxia.txt", "w") as f:
                        f.writelines(other_list)
                    break

                current_row = current_row.replace("\\N", "")
                item_list = current_row.split("---")
                temp_name = item_list[0].strip()
                if is_all_chinese(temp_name):
                    all_chinese_name_list.append(current_row)
                    all_chinese_name_rows_num = all_chinese_name_rows_num + 1
                else:
                    other_list.append(current_row)
                    other_rows_num = other_rows_num + 1

            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % total_rows)
                break


#检验是否全是中文字符
def is_all_chinese(strs):
    for _char in strs:
        if not '\u4e00' <= _char <= '\u9fa5':
            return False
    return True


def handle_mobilephone_records():

    baijiaxing = Baijiaxing()

    file_full_path = r"D:\FFOutput\has_phone.txt"
    with open(file_full_path, "r") as file_handle:
        total_rows = 0
        other_list = []
        kkkk_list = []
        unkwown_dict = {}
        has_phone_list = []
        has_phone_rows_num = 0
        other_rows_num = 0
        while True:
            try:
                total_rows = total_rows + 1
                if total_rows % 10000 == 0:
                    print("已经读取%d行" % total_rows)

                current_row = file_handle.readline()
                if not current_row:
                    print("文件读取完成，共读取数据%d行" % (total_rows-1) + ",有移动电话记录%d行" % has_phone_rows_num
                          + ", 其他%d行" % other_rows_num)
                    print("has_phone_list_list:据%d行" % len(has_phone_list)
                          + ",other_list:%d行" % len(other_list))
                    with open("D:/FFOutput/valid.txt", "w",encoding="utf8") as has_phone_file:
                        has_phone_file.writelines(has_phone_list)
                    with open("D:/FFOutput/other.txt", "w") as f:
                        f.writelines(other_list)

                    with open("D:/FFOutput/unknown.txt", "w") as fk:
                        fk.writelines(kkkk_list)
                    break

                current_row = current_row.replace("\\N", "")
                item_list = current_row.split("---")
                temp_name = item_list[0].strip()
                if len(temp_name) < 2:
                    other_list.append(current_row)
                    other_rows_num = other_rows_num + 1
                    continue

                name_first_word = temp_name[0]
                name_second_word = temp_name[1]

                is_baijiaxing = baijiaxing.is_one_of_baijiaxing(name_first_word)
                if is_baijiaxing == False:
                    is_baijiaxing = baijiaxing.is_one_of_baijiaxing(name_first_word + name_second_word)
                """
                if is_baijiaxing == False:
                    unkwown_dict[name_first_word+"\n"] = name_first_word + "\n"
                """
                has_mobile_phone = False
                for i in range(len(item_list)):
                    if i < 3:
                        continue
                    if len(item_list[i].strip()) == 11:
                        try:
                            temp_int = int(item_list[i].strip())
                            if len(str(temp_int)) == 11:
                                has_mobile_phone = True
                                break
                        except Exception as me:
                            has_mobile_phone = False
                            # print(me)

                if has_mobile_phone is True and is_baijiaxing is False:
                    tempstr = name_first_word + name_second_word
                    if tempstr == "手机" or tempstr == "团购" or name_first_word == "老":
                        mm = tempstr
                    else:
                        if len(temp_name) == 3:
                            kkkk_list.append(current_row)

                if has_mobile_phone and is_baijiaxing:
                    has_phone_list.append(current_row)
                    has_phone_rows_num = has_phone_rows_num + 1
                else:
                    other_list.append(current_row)
                    other_rows_num = other_rows_num + 1

            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % total_rows)
                break

def save_other_records():

    mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager.open_connect()
    baijiaxing = Baijiaxing()

    file_full_path = r"D:\FFOutput\jingdong-buchongshuju.txt"
    datarow_list = []
    inserted_num = 0
    tuangou_num = 0
    insert_sql_stmt = "INSERT INTO jingdong (NAME,LOGIN_NAME,EMAIL,ID_NO,PHONE,TELEPHONE) VALUES(%s,%s,%s,%s,%s,%s)"

    with open(file_full_path, "r", encoding="utf8") as file_handle:
        total_rows = 0
        while True:
            try:
                total_rows = total_rows + 1

                current_row = file_handle.readline()
                if not current_row:

                    temp_count = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_count
                    datarow_list.clear()
                    print("文件读取完成，共读取数据%d行" % (total_rows - 1) + ",插入%d行" % inserted_num)
                    print("团购订单数量%d"% tuangou_num)
                    break

                current_row = current_row.replace("\\N", "")
                item_list = current_row.split("---")
                temp_name = item_list[0].strip()

                if temp_name.find("团购") >= 0:
                    tuangou_num = tuangou_num + 1
                    continue

                temp_login_name = item_list[1].strip()  # 登录用户
                temp_email = ""
                temp_phone = ""
                temp_id_no = ""
                for i in range(len(item_list)):
                    if i < 2:
                        continue

                    if len(item_list[i].strip()) == 0:
                        continue

                    #判断是不是手机号字段
                    if len(item_list[i].strip()) == 11:
                        try:
                            temp_int = int(item_list[i].strip())
                            if len(str(temp_int)) == 11:
                                temp_phone = str(temp_int)
                        except Exception as me:
                            temp_phone = ""

                    #判断是不是电子邮箱
                    if item_list[i].strip().find("@") > 0:
                        temp_email = item_list[i].strip()

                    if len(item_list[i].strip()) == 15 or len(item_list[i].strip()) == 18:
                        temp_id_no = item_list[i].strip()

                temp_telephone = ""

                mysql_values_tuple = (temp_name, temp_login_name, temp_email, temp_id_no, temp_phone, temp_telephone)
                datarow_list.append(mysql_values_tuple)
                if len(datarow_list) == 100000:
                    temp_num = batch_insert_data(mariadb_manager.connect, insert_sql_stmt, datarow_list)
                    inserted_num = inserted_num + temp_num
                    datarow_list.clear()
                    print("共计插入数据" + str(inserted_num) + "行")

            except Exception as ex:
                print(ex)
                print("程序异常退出，已经插入数据%d行" % total_rows)
                break


def update_mobile_phone():

    # mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager = MariadbManager("192.168.1.116", 3308, "privacydata", "root", "Springdawn@2016", charset="utf8mb4")
    mariadb_manager.open_connect()
    total_rows_num = 0
    cur_row_num = 218857935
    inserted_num = 0
    block_len = 100000
    randam_num = 0
    query_total = 0
    while True:
        try:
            print(cur_row_num)
            query_sql = "SELECT jingdong_id,telephone FROM jingdong where  jingdong_id >  " + str(cur_row_num) + " and jingdong_id <= "
            query_sql = query_sql + str(cur_row_num + block_len) + " and phone='' order by jingdong_id "
            query_total = query_total + block_len
            mysql_query_cursor = mariadb_manager.connect.cursor()

            mysql_query_cursor.execute(query_sql)
            total_rows_num = total_rows_num + mysql_query_cursor.rowcount

            if cur_row_num > 227485998:
                print("遍历完成")
                break

            if mysql_query_cursor.rowcount == 0:
                cur_row_num = cur_row_num + block_len
                continue

            result_row = mysql_query_cursor.fetchall()
            datarow_list = []
            for cur_row in result_row:
                temp_jingdong_id = cur_row[0]
                temp_telphone = cur_row[1]
                if len(temp_telphone) == 11:
                    if temp_telphone[0] == "1":
                        datarow_list.append([temp_telphone, temp_jingdong_id])
                elif len(temp_telphone) == 12:
                    if temp_telphone[0:2] == "01":
                        try:
                            temp_phone_int = int(temp_telphone)
                            if len(str(temp_phone_int)) == 11:
                                datarow_list.append([str(temp_phone_int), temp_jingdong_id])
                        except Exception as ec:
                            randam_num = randam_num + 1
            cur_row_num = cur_row_num + block_len
            mysql_query_cursor.close()

            this_update_num = 0
            if len(datarow_list) > 0:
                print(datarow_list[0])
                update_cursor = mariadb_manager.connect.cursor()
                update_sql = "UPDATE jingdong SET phone = (%s) WHERE jingdong_id = (%s)"
                update_cursor.executemany(update_sql, datarow_list)
                mariadb_manager.connect.commit()
                this_update_num = update_cursor.rowcount
                inserted_num = inserted_num + update_cursor.rowcount
                update_cursor.close()

            print("已经查询总行数%d"%query_total + " 需要更新行数%d"%len(datarow_list)
                  + "  本次完成更新%d"%this_update_num + " 累计更新行数%d"%inserted_num)
            datarow_list.clear()
        except Exception as ex:
            print(ex)
            print("程序异常退出，已经插入数据%d行" % inserted_num)
            break

    mariadb_manager.close_connection()


def extract_valid_qq():

    soyun_manager = MariadbManager("192.168.1.116", 3308, "privacydata", "root", "Springdawn@2016", charset="utf8mb4")
    soyun_manager.open_connect()

    insertdb_manager = MariadbManager("192.168.1.116", 3308, "soyun", "root", "Springdawn@2016", charset="utf8mb4")
    insertdb_manager.open_connect()

    insert_sql_stmt = "INSERT INTO temp (qq_no,phone,info_source)"
    insert_sql_stmt = insert_sql_stmt + "VALUES(%s,%s,%s)"

    cur_row_num = 122157565
    inserted_num = 0
    block_len = 100000
    while True:
        try:

            if cur_row_num > 227587801:
                print("累计插入数据共计:%d行" % inserted_num)
                break

            query_sql = "SELECT email,phone FROM jingdong  where jingdong_id > " + str(cur_row_num)
            query_sql = query_sql + " and jingdong_id <= " + str(cur_row_num + block_len) + " and email like '%qq.com%'"
            query_sql = query_sql + " order by jingdong_id "

            soyun_query_cursor = soyun_manager.connect.cursor()
            soyun_query_cursor.execute(query_sql)
            result_row = soyun_query_cursor.fetchone()
            datarow_list = []
            while result_row:

                temp_phone = result_row[1].strip()
                if len(temp_phone) == 0:
                    result_row = soyun_query_cursor.fetchone()
                    continue

                temp_email = result_row[0].strip()
                temp_pos = temp_email.find("@")
                temp_qq = temp_email[0:temp_pos]
                temp_qq_int = 0
                try:
                    temp_qq_int = int(temp_qq)
                except Exception as intex:
                    # print(intex)
                    result_row = soyun_query_cursor.fetchone()
                    continue
                if len(str(temp_qq_int)) > 1:
                    item_tuple = (temp_qq, temp_phone, "jingdong")
                    datarow_list.append(item_tuple)
                result_row = soyun_query_cursor.fetchone()

            soyun_query_cursor.close()
            if len(datarow_list) > 0:
                temp_num = batch_insert_data(insertdb_manager.connect, insert_sql_stmt, datarow_list)
                inserted_num = inserted_num + temp_num
                datarow_list.clear()
                print("累计插入数据共计:%d行" % inserted_num)
            cur_row_num = cur_row_num + block_len

        except Exception as ex:
            print(ex)
            print("程序异常退出，已经插入数据%d行" % inserted_num)
            break


def diff_qq_vs_jingdong():

    qq_manager = MariadbManager("192.168.1.116", 3308, "privacydata", "root", "Springdawn@2016", charset="utf8mb4")
    qq_manager.open_connect()

    query_manager = MariadbManager("192.168.1.116", 3308, "soyun", "root", "Springdawn@2016", charset="utf8mb4")
    query_manager.open_connect()

    insert_manager = MariadbManager("192.168.1.116", 3308, "soyun", "root", "Springdawn@2016", charset="utf8mb4")
    insert_manager.open_connect()

    insert_sql_stmt = "INSERT INTO diffqq (qq_no,phone,info_source)"
    insert_sql_stmt = insert_sql_stmt + "VALUES(%s,%s,%s)"

    # 10048950
    cur_row_num = 13648950
    inserted_num = 0
    block_len = 100000
    while True:
        try:
            if cur_row_num > 30632833:
                print("累计插入数据共计:%d行" % inserted_num)
                break
            query_sql = "SELECT qq_no,phone FROM temp  where temp_id > " + str(cur_row_num)
            query_sql = query_sql + " and temp_id <= " + str(cur_row_num + block_len) + " order by temp_id "
            print(query_sql)
            query_cursor = query_manager.connect.cursor()
            query_cursor.execute(query_sql)
            datarow_list = []
            for temp_index in range(query_cursor.rowcount):

                result_row = query_cursor.fetchone()
                temp_qq_no = result_row[0].strip()
                temp_phone = result_row[1].strip()
                if len(temp_qq_no) > 10:
                    continue

                qq_query_sql = "SELECT phone FROM qq  where qq_no='" + temp_qq_no + "'"
                qq_query_cursor = qq_manager.connect.cursor()
                qq_query_cursor.execute(qq_query_sql)
                tempcount = qq_query_cursor.rowcount
                if tempcount == 0:
                    item_tuple = (temp_qq_no, temp_phone, "jingdong")
                    datarow_list.append(item_tuple)
                else:
                    temp_phone_list = []
                    for i in range(tempcount):
                        # 获取查询的结果
                        qq_cur_row = qq_query_cursor.fetchone()
                        temp_phone_list.append(qq_cur_row[0])
                    if temp_phone not in temp_phone_list:
                        item_tuple = (temp_qq_no, temp_phone, "jingdong")
                        datarow_list.append(item_tuple)
                qq_query_cursor.close()
            query_cursor.close()
            print("开始insert")
            if len(datarow_list) > 0:
                temp_num = batch_insert_data(insert_manager.connect, insert_sql_stmt, datarow_list)
                inserted_num = inserted_num + temp_num
                datarow_list.clear()
                print("累计插入数据共计:%d行" % inserted_num)
            cur_row_num = cur_row_num + block_len
        except Exception as ex:
            print(ex)
            print("程序异常退出，已经插入数据%d行" % inserted_num)
            break


def main():

    try:
        diff_qq_vs_jingdong()
        # update_mobile_phone()
        # parse_jingdong_data()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
