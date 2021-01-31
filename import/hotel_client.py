import csv
import time


def transfer_date():

    file_full_path = r"E:\Downloads\2000W\最后5000.csv"
    with open(file_full_path, "r", encoding="utf8") as file_handler:
        csv_reader = csv.reader(file_handler)
        for row in csv_reader:
            print(row)


def main():

    try:
        transfer_date()
    except Exception as ex:
        print(ex)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('Beginning ! Begin:' + start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print('\nEnd:' + end_time)
