import openpyxl
import time
import os
from mariadb.mariadb_manager import MariadbManager


def parse_huji_excel(excel_full_path, mariadb_conn, insert_sql_stmt):

    excel_book = openpyxl.load_workbook(excel_full_path, read_only=True)
    excel_sheet = excel_book.worksheets[0]

    print("总行数：" + str(excel_sheet.max_row))
    print("总列数：" + str(excel_sheet.max_column))

    insert_sql_stmt = "INSERT INTO HUJI (STAT_TIME, STAT_NO, BIRTH_DATE, SEX, ID_NO, NMAE, PHONE, ADDRESS)"
    insert_sql_stmt = insert_sql_stmt + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    datarow_list = []
    row_num = 0
    for cur_row in excel_sheet.rows:
        row_num = row_num + 1
        temp_stat_time = cur_row[2].value.strip()   # cur_row[2]  统计时间
        temp_stat_no = cur_row[4].value.strip()     # cur_row[4]  编码编号
        temp_birth_date = cur_row[5].value.strip()  # cur_row[5]  出生日期
        temp_sex = cur_row[6].value.strip()         # cur_row[6]  性别
        temp_id_no = cur_row[7].value.strip()       # cur_row[7]  身份证号
        temp_name = cur_row[8].value.strip()        # cur_row[8]  姓名
        temp_phone = cur_row[9].value.strip()       # cur_row[9]  电话号码
        temp_address = cur_row[10].value.strip()     # cur_row[10]  地址

        if len(temp_phone) == 0:
            print("用户的电话号码为空，本条数据跳过" % temp_name)
            continue

        mysql_values_tuple = (temp_stat_time, temp_stat_no, temp_birth_date,
                              temp_sex, temp_id_no, temp_name, temp_phone, temp_address)
        datarow_list.append(mysql_values_tuple)
        if len(datarow_list) == 10000:
            temp_num = batch_insert_data(mariadb_conn, insert_sql_stmt, datarow_list)
            inserted_num = inserted_num + temp_num
            datarow_list.clear()
            print("共计插入数据%d行" % inserted_num)

    if len(datarow_list) > 0:
        temp_count = batch_insert_data(mariadb_conn, insert_sql_stmt, datarow_list)
        inserted_num = inserted_num + temp_count
        print("共计插入数据%d行" % inserted_num)
    print("文件入库完成，共入库数据"+str(inserted_num)+"行")


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


def check_huji_file_format(full_path, column_header_list):

    excel_book = openpyxl.load_workbook(full_path, read_only=True)
    excel_sheet = excel_book.worksheets[0]
    if excel_sheet.max_row > 1:
        is_same = True
        same_num = 0
        diff_column_name = ""
        temp = ""
        for i in range(len(column_header_list)):
            if excel_sheet.cell(1, i+1).value == column_header_list[i]:
                temp = temp + excel_sheet.cell(1, i + 1).value + ","
                same_num = same_num + 1
            else:
                is_same = False
                diff_column_name = diff_column_name
                break

        if is_same is False:
            print("文件:"+full_path+" 的列头与标准列头不一致，请检查")
            print(diff_column_name)
        else:
            print("文件:" + full_path + " 的格式无误")

        temp_id_no = excel_sheet.cell(2, 7).value.strip()  # cur_row[6]  身份证号
        temp_name = excel_sheet.cell(2, 8).value.strip()  # cur_row[7]  姓名
        temp_phone = excel_sheet.cell(2, 9).value.strip()  # cur_row[8]  电话号码
        if len(temp_id_no) != 15:
            if len(temp_id_no) != 18:
                print("身份证格式有误:%s" %temp_id_no)

    else:
        print("文件:" + full_path + " 的行数为空，请检查！")


def parse_all_huji_data():

    mariadb_manager = MariadbManager("127.0.0.1", 3306, "privacydata", "root", "pmo@2016",  charset="utf8mb4")
    mariadb_manager.open_connect()

    column_header = ["所属户籍站", "统计时间", "居住地址", "编码编号", "出生日期", "性别", "身份证", "名字", "手机号", "地址", "编码", "编码"]
    rootdir = r"D:\downloads\privacydata\huji"
    list = os.listdir(rootdir)  # 列出文件夹下所有的目录与文件
    file_num = 0
    for i in range(0, len(list)):
        file_path = os.path.join(rootdir, list[i])
        if os.path.isfile(file_path):
            file_num = file_num + 1
            print("开始第"+ str(file_num) + "文件:" + file_path + "！")
            check_huji_file_format(file_path, column_header)
            # parse_huji_excel(file_path)
            print()

def main():

    try:
        parse_all_huji_data()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
