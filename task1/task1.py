import pandas as pd
import numpy as np 
import os,shutil
from parameters import *
import pyodbc
import datetime

starttime = datetime.datetime.now() #程序开始执行时间

#create_data函数利用数据生成器文件在指定目录下生成指定大小的tbl文件
def create_data(args):  
    #利用dbgen数据生成器生成tbl文件
    dstfilelist = ['./dbgen','./dists.dss']
    for i,item in enumerate(dstfilelist):
        if not os.path.isfile(item):
            print('不存在数据生成器文件！')
            raise   #如果不存在数据生成器文件则报错
        if not os.path.exists(args.dir):
            os.makedirs(args.dir)                #创建路径
    os.system('./dbgen -s ' + str(args.sf)) #利用数据生成器生成指定参数大小的tbl文件，并且生成到指定路径下

    #将生成的tbl文件移动到指定目录下
    print('\n')
    old_path = './'
    new_path = args.dir
    items = os.listdir("./")
    filelist = []
    for names in items:
        if names.endswith(".tbl"):
            filelist.append(names)
    count = 0
    for i in filelist:
        count =count+1
        src = os.path.join(old_path, i)
        dst = os.path.join(new_path, i)
        shutil.move(src, dst)
        print('creating {}'.format(i))
    print('\n共生成{}个文件'.format(count))


#clean_tbl文件将处理tbl文件中的末尾竖线
def clean_data(args):
    #处理数据文件末尾的竖线
    print('\n')
    count = 0
    items = os.listdir(args.dir)
    filelist = []
    for names in items:
        if names.endswith(".tbl"):
            filelist.append(names)
    for item in filelist:
        count = count+1
        f = open(args.dir+'/'+item, "r",encoding='utf-8')
        lines = f.readlines()
        f.close()
        for i in range(len(lines)):
            lines[i] = lines[i].replace('|\n','\n')  #去掉每行末尾竖线
        f = open(args.dir+'/'+item, "w",encoding='utf-8')
        f.writelines(lines)
        f.close()
        print('处理{}的末尾竖线'.format(item))
    print('共处理{}个文件'.format(count))


#create_tbl函数连接数据库，并在数据库中生成数据表并插入刚刚的tbl文件
def create_tbl(args):
    #连接sqlserver数据库，并创建相应数据表
    print('\n创建数据库：')
    #连接sqlserver
    conn = pyodbc.connect(
        r'DRIVER={ODBC Driver 17 for SQL Server};' 
        #注：driver这里这样写即表明是用driver连接而不是用dsn，如果这里直接写sqlserver会在linux连接不了，只能在windows连接，linux连接需要写上driver名字
        r'SERVER=localhost;'
        # r'DATABASE=TPCH;' #因为连接该库即不可以drop该表，因此选择注释掉
        r'UID=sa;'
        r'PWD=*******' #这里使用密码
        )
    cursor=conn.cursor() #创建一个cursor对象，即游标对象，利用cursor中的命令来实现操作

    #读取sql文件，来执行sql命令
    conn.autocommit = True #开启自动提交，才可以执行数据库的建立和创建
    filepath = './create_tpch.sql' #读取create_tpch数据文件，执行其中对应数据表的创建
    with open(filepath,mode='r') as f:
        sql_list = f.read().split(';')[:-1]
        for x in sql_list:
            cursor.execute(x)
    conn.autocommit = False
    print('数据库创建完成')

    #读取对应路径下的tbl文件，插入数据表中
    print('\n')
    items = os.listdir(args.dir)
    filelist = []
    for names in items:
        if names.endswith(".tbl"): #选取路径下所有的tbl文件
            filelist.append(names)
    count = 0
    for i in filelist:
        count = count+1
        item = '/home/xinyi/ch2/task1'+args.dir.split('.')[1] +'/' +i
        tbl = i.split('.')[0].upper()
        print('插入'+tbl+'到数据库中')
        cursor.execute("BULK INSERT " + tbl+  " FROM '" + item + "' WITH ( \
                            FIELDTERMINATOR = '|', ROWTERMINATOR = '\n')")
    print('插入数据完成，共插入{}个表格'.format(count))

    #利用select语句查看数据表是否创建成功
    print('\n')
    count = 0 
    cursor.execute("USE TPCH;")
    for i in filelist:
        tbl = i.split('.')[0].upper()
        cursor.execute("SELECT COUNT(*) FROM "+tbl)
        rows = cursor.fetchone()[0]
        if rows == 0:
            count = count +1 #即如果有一个表的行数为0，即说明插入没成功，则令计数器加1
            print(tbl +' 行数:{}'.format(rows)+ 'error!!')
        else:
            print(tbl +' 行数:{}'.format(rows))
    print('未成功创建的数据表个数：{}'.format(count))
               
    conn.commit()
    cursor.close()#关闭游标对象
    conn.close()#断开数据库连接




def main(args):
    create_data(args)
    clean_data(args)
    create_tbl(args)


if __name__ == '__main__':
    args = task_parser()
    main(args)

endtime = datetime.datetime.now()
print('\n程序运行时间：{}s'.format((endtime - starttime).seconds))
    

    


