# coding=utf-8;
# 导入模块pymysql模块
import pymysql


# 封装Mysql数据库管理类
class MariadbManager(object):
    # 初始化方法
    def __init__(self, host, port, database, user, password, charset):
        # 配置连接MySQL数据库的基本信息
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.charset = charset

    # 使用python3连接MySQL数据库
    def open_connect(self):

        try:
            self.connect = pymysql.connect(host=self.host, port=self.port, database=self.database, user=self.user,
                                           password=self.password, charset=self.charset)
            self.cursor = self.connect.cursor()
        except Exception as ex:
            print(ex)

    # 操作完毕后关闭
    def close_connection(self):
        # 关闭执行语句
        self.cursor.close()
        # 关闭连接
        self.connect.close()


    # 创建表操作
    def create_table(self, sql, params=()):
        # 先连接
        self.open_connect()
        # 执行创建语句
        self.cursor.execute(sql, params)
        # 关闭连接
        self.close_connection()

    # 查询一条数据
    def select_one(self, sql, params=()):
        result = None
        try:
            self.open_connect()
            self.cursor.execute(sql, params)
            result = self.cursor.fetchone()
            self.close_connection()
        except Exception as e:
            print(e)
        return result

    # 查询全部数据
    def select_all(self, sql, params=()):
        list = ()
        try:
            self.open_connect()
            self.cursor.execute(sql, params)
            list = self.cursor.fetchall()
            self.close_connection()
        except Exception as e:
            print(e)
        return list

    # 插入
    def insert(self, sql, params=()):
        return self.__edit(sql, params)

    # 修改
    def update(self, sql, params=()):
        return self.__edit(sql, params)

    # 删除
    def delete(self, sql, params=()):
        return self.__edit(sql, params)

    # 插入、修改、删除其实一样的，只是sql代码不同，但是为了代码的阅读性更高，还是分开写
    def __edit(self, sql, params):
        count = 0
        try:
            self.open_connect()
            count = self.cursor.execute(sql, params)
            self.connect.commit()
            self.close_connection()
        except Exception as e:
            print(e)
        return count