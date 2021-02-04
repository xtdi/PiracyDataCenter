import csv
import datetime
import os
import time
from mariadb.mariadb_manager import MariadbManager


def transfer_data():
    # mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016", charset="utf8mb4")
    mariadb_manager = MariadbManager("192.168.1.116", 3308, "privacydata", "root", "Springdawn@2016", charset="utf8mb4")
    mariadb_manager.open_connect()

    table_field_list = ["name", "card_no", "descriot", "ctf_tp", "ctf_Id", "gender", "birthday", "address"]
    table_field_list.extend(["zip", "dirty", "district1", "district2", "district3", "district4", "district5"])
    table_field_list.extend(["district6", "first_name", "last_name", "duty", "mobile", "tel", "fax", "email", "nation"])
    table_field_list.extend(["taste", "education", "company", "ctel", "caddress", "czip", "family", "version", "id"])

    fieldname_list_str = ""
    fieldvalue_exp_str = ""

    for i in range(len(table_field_list)):
        if i == 0:
            fieldname_list_str = table_field_list[i]
            fieldvalue_exp_str = "%s"
        else:
            fieldname_list_str = fieldname_list_str + ", " + table_field_list[i]
            fieldvalue_exp_str = fieldvalue_exp_str + ", " + "%s"

    insert_sql_stmt = "INSERT INTO hotel_client (" + fieldname_list_str + ")VALUES(" + fieldvalue_exp_str + ")"


    """
    insert_sql_stmt = "INSERT INTO hotel_client(name, card_no, descriot, ctf_tp, ctf_Id, gender,birthday,address,zip"
    insert_sql_stmt = insert_sql_stmt + ",dirty,district1,district2,district3,district4,district5,district6,first_name"
    insert_sql_stmt = insert_sql_stmt + ",last_name,duty,mobile,tel,fax,email,nation,taste,education,company,ctel"
    insert_sql_stmt = insert_sql_stmt + ",caddress,czip, family,version,id"
    insert_sql_stmt = insert_sql_stmt + ")VALUES(" + fieldvalue_exp_str + ")"
    """

    rootdir = r"D:\downloads\temp\new"
    file_list = os.listdir(rootdir)  # 列出文件夹下所有的目录与文件
    file_num = 0
    for i in range(0, len(file_list)):
        file_path = os.path.join(rootdir, file_list[i])
        if os.path.isfile(file_path):
            file_num = file_num + 1
            print("开始第" + str(file_num) + "文件:" + file_path + "！")
            parse_each_file(file_path, mariadb_manager.connect, insert_sql_stmt, table_field_list)


def parse_each_file(file_full_path, mariadbconn, insert_sql_stmt, table_field_list):

    inserted_num = 0
    rows_num = 0
    invalid_rows = []
    datarow_list = []
    null_rows_num = 0
    # file_full_path = r"E:\Downloads\2000W\1-200W.csv"
    with open(file_full_path, "r", encoding="utf-8-sig") as file_handler:
        csv_reader = csv.reader(file_handler)
        for cur_row in csv_reader:
            rows_num = rows_num + 1

            if rows_num == 1:
                temp_card_no = cur_row[1]
                if temp_card_no == "card_no":
                    # 存在标题行
                    continue

            if rows_num < 5:
                continue
            if len(cur_row) == 0:
                null_rows_num = null_rows_num + 1
                continue

            try:
                cur_row_value_tuple = build_insert_values(cur_row)
                tuple_len = len(cur_row_value_tuple)
                if tuple_len < 33:
                    invalid_rows.append(cur_row)
                    continue
                datarow_list.append(cur_row_value_tuple)
            except Exception as ex:
                # print(ex)
                # if ex.args[0].find("unconvert") >= 0:
                #    print()
                invalid_rows.append(cur_row)
                continue
            if len(datarow_list) == 100000:
                # print("已经读取%d"% rows_num)
                temp_num = batch_insert_data(mariadbconn, insert_sql_stmt, datarow_list)
                inserted_num = inserted_num + temp_num
                datarow_list.clear()
                print("已经插入数据共计:%d行" % inserted_num + ",已经读取物理行共计:%d" % rows_num)
    if len(datarow_list) > 0:
        print("插入未插入的数据行：%d"%len(datarow_list))
        temp_num = batch_insert_data(mariadbconn, insert_sql_stmt, datarow_list)
        inserted_num = inserted_num + temp_num
        datarow_list.clear()
    print("已经插入数据共计:%d行" % inserted_num + ",已经读取物理行共计:%d" % rows_num)
    print("无效数据行：%d" % len(invalid_rows))
    print("空数据行：%d" % null_rows_num)

    file_name = os.path.split(file_full_path)[-1]

    with open("D:\\invalid-"+file_name, mode="w", encoding="utf-8-sig") as out_file:
        writer = csv.writer(out_file)
        writer.writerow(table_field_list)
        for temp_item in invalid_rows:
            writer.writerow(temp_item)
        print("写入完毕！")


def build_insert_values(field_value_list):

    mysql_values_tuple = ()
    for i in range(len(field_value_list)):
        temp_item = field_value_list[i].strip()
        if len(temp_item) == 0:
            mysql_values_tuple = mysql_values_tuple + (None, )
            continue

        if i == 6:
            birth_date = None
            try:
                birth_date = datetime.datetime.strptime(temp_item, "%Y%m%d")
            except Exception  as ecx:
                birth_date = None
            mysql_values_tuple = mysql_values_tuple + (birth_date,)
        elif i == 32:
            temp_id = int(temp_item)
            mysql_values_tuple = mysql_values_tuple + (temp_id,)
        else:
            mysql_values_tuple = mysql_values_tuple + (temp_item,)
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


def handle_kuahang_format(file_full_path):
    table_field_list = ["name", "card_no", "descriot", "ctf_tp", "ctf_Id", "gender", "birthday", "address"]
    table_field_list.extend(["zip", "dirty", "district1", "district2", "district3", "district4", "district5"])
    table_field_list.extend(["district6", "first_name", "last_name", "duty", "mobile", "tel", "fax", "email", "nation"])
    table_field_list.extend(["taste", "education", "company", "ctel", "caddress", "czip", "family", "version", "id"])

    new_row_list = []
    with open(file_full_path, mode="r", encoding="utf-8-sig") as file_handler:
        csv_reader = csv.reader(file_handler)
        row_num = 0
        last_row = []
        format_true_num = 0
        need_kuahang = False
        for cur_row in csv_reader:
            row_num = row_num + 1
            if len(cur_row) == 33:
                format_true_num = format_true_num + 1
            if need_kuahang:
                for num in range(7, 33):
                    last_row[num] = cur_row[num-7]
                new_row_list.append(last_row)
                last_row = cur_row
                need_kuahang = False
                continue
            else:
                last_row = cur_row
                if len(cur_row[32].strip()) == 0:
                    other_element_is_null = True
                    for num in range(8, 32):
                        if len(cur_row[num].strip()) > 0:
                            other_element_is_null = False
                            break
                    if other_element_is_null == True:
                        need_kuahang = True
                if need_kuahang:
                    continue
                else:
                    new_row_list.append(cur_row)

        print(format_true_num)
        if row_num == format_true_num + 1:
            print("所有行的列数都符合数量要求")
    file_dir = os.path.dirname(file_full_path)
    new_file_dir = file_dir+"\\new"
    isExists = os.path.exists(new_file_dir)
    if not isExists:
        os.makedirs(new_file_dir)
    file_name = os.path.split(file_full_path)[-1]
    with open(new_file_dir + "\\" +file_name, mode="w", encoding="utf-8-sig") as out_file:
        writer = csv.writer(out_file)
        writer.writerow(table_field_list)
        for temp_item in new_row_list:
            writer.writerow(temp_item)
        print("写入完毕！")


def update_mobile_phone():

    mariadb_manager = MariadbManager("192.168.1.116", 3308, "privacydata", "root", "Springdawn@2016", charset="utf8mb4")
    mariadb_manager.open_connect()
    total_rows_num = 0
    cur_row_num = 0
    inserted_num = 0
    block_len = 100000
    randam_num = 0
    query_total = 0
    while True:
        try:
            print(str(cur_row_num) +"  读数据开始：" + str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
            query_sql = "SELECT hotel_client_id,tel FROM hotel_client where  hotel_client_id >  " + str(cur_row_num) + " and hotel_client_id <= "
            query_sql = query_sql + str(cur_row_num + block_len) + " and (mobile='' or mobile is null)  order by hotel_client_id "
            query_total = query_total + block_len
            mysql_query_cursor = mariadb_manager.connect.cursor()

            mysql_query_cursor.execute(query_sql)
            total_rows_num = total_rows_num + mysql_query_cursor.rowcount

            if cur_row_num > 20050000:
                print("遍历完成")
                break

            if mysql_query_cursor.rowcount == 0:
                cur_row_num = cur_row_num + block_len
                continue

            result_row = mysql_query_cursor.fetchall()
            datarow_list = []
            for cur_row in result_row:
                temp_hotel_client_id = cur_row[0]
                temp_telphone = cur_row[1]
                if temp_telphone is None:
                    continue
                if len(temp_telphone) == 11:
                    if temp_telphone[0] == "1":
                        datarow_list.append([temp_telphone, temp_hotel_client_id])
                elif len(temp_telphone) == 12:
                    if temp_telphone[0:2] == "01":
                        try:
                            temp_phone_int = int(temp_telphone)
                            if len(str(temp_phone_int)) == 11:
                                datarow_list.append([str(temp_phone_int), temp_hotel_client_id])
                        except Exception as ec:
                            randam_num = randam_num + 1
            cur_row_num = cur_row_num + block_len
            mysql_query_cursor.close()
            this_update_num = 0
            if len(datarow_list) > 0:
                print("插入数据开始：" + str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
                print(datarow_list[0])
                update_cursor = mariadb_manager.connect.cursor()
                update_sql = "UPDATE hotel_client SET mobile = (%s) WHERE hotel_client_id = (%s)"
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

def main():

    try:
        update_mobile_phone()
        # handle_kuahang_format(r"D:\downloads\temp\invalid-最后5000.csv")
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
