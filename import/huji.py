import openpyxl
import time
import os


def parse_huji_excel(excel_full_path):

    excel_book = openpyxl.load_workbook(excel_full_path, read_only=True)
    excel_sheet = excel_book.worksheets[0]

    print("总行数：" + str(excel_sheet.max_row))
    print("总列数：" + str(excel_sheet.max_column))
    insert_sql_pre = "INSERT INTO HUJI (STAT_TIME,STAT_NO,BIRTH_DATE,SEX,ID_NO,NMAE,PHONE,ADDRESS) VALUES ("
    datarow_list = []
    row_num = 0
    for cur_row in excel_sheet.rows:
        row_num = row_num + 1
        item_value = "\""
        # cur_row[1]  统计时间
        temp_stat_time = cur_row[1].value.strip()
        item_value = item_value + temp_stat_time
        item_value = item_value + "\",\""
        # cur_row[3]  编码编号
        temp_stat_no = cur_row[3].value.strip()
        item_value = item_value + temp_stat_no
        item_value = item_value + "\",\""
        # cur_row[4]  出生日期
        temp_birth_date = cur_row[4].value.strip()
        item_value = item_value + temp_birth_date
        item_value = item_value + "\",\""
        # cur_row[5]  性别
        temp_sex = cur_row[5].value.strip()
        item_value = item_value + temp_sex
        item_value = item_value + "\",\""
        # cur_row[6] 身份证号
        temp_id_no = cur_row[6].value.strip()
        item_value = item_value + temp_id_no
        item_value = item_value + "\",\""
        # cur_row[7] 姓名
        temp_name = cur_row[7].value.strip()
        item_value = item_value + temp_name
        item_value = item_value + "\",\""
        # cur_row[8] 电话号码
        temp_phone = cur_row[8].value.strip()
        item_value = item_value + temp_phone
        item_value = item_value + "\",\""
        # cur_row[9] 地址
        temp_address = cur_row[9].value.strip()
        item_value = item_value + temp_address
        item_value = item_value + "\"）"

        mysql_values_tuple = (temp_stat_time,temp_stat_no, temp_birth_date,
                              temp_sex, temp_id_no, temp_name, temp_phone, temp_address)
        datarow_list.append(mysql_values_tuple)
        # print( insert_sql_pre + item_value )
    print("文件入库完成，共入库数据"+str(0)+"条")

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

    else:
        print("文件:" + full_path + " 的行数为空，请检查！")



def parse_huji_data():

    column_header = ["所属户籍站", "统计时间", "居住地址", "编码编号", "出生日期", "性别", "身份证", "名字", "手机号", "地址", "编码", "编码"]
    rootdir = r"D:\Downloads\户籍数据1.03GB"
    list = os.listdir(rootdir)  # 列出文件夹下所有的目录与文件
    file_num = 0
    for i in range(0, len(list)):
        file_path = os.path.join(rootdir, list[i])
        if os.path.isfile(file_path):
            file_num = file_num + 1
            print("开始第"+ str(file_num) + "文件:" + file_path + "！")
            # check_huji_file_format(file_path, column_header)
            parse_huji_excel(file_path)
            print()

def main():

    try:
        parse_huji_data()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
