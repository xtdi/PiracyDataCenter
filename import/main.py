# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import csv


def parse_each_file():

    rows_num = 0
    file_full_path = r"D:\迅雷下载\back\2000W开房数据shifenzheng.csv"
    with open(file_full_path, "r", encoding="utf-8-sig") as file_handler:
        csv_reader = csv.reader(file_handler)
        for cur_row in csv_reader:
            rows_num = rows_num + 1




def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parse_each_file()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/





