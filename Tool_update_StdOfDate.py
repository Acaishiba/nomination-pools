import pymysql
import datetime
import math
import statistics
#the_date为指定的需要刷新内容的日期，date_range为需要计算多长时间内的标准差
def update_std(the_date, date_range):
    # 连接到数据库
    conn = pymysql.connect(
        host="127.0.0.1",
        user="xxxxxx",
        password="xxxxxxx",
        database="pools"
    )

    # 初始化时间
    date_str = the_date.strftime("%Y-%m-%d")

    # 确定分子分母列表
    apr_table_name = 'Pool_APR'
    std_table_name = 'Pool_std'

    start_date = the_date - datetime.timedelta(days=date_range)
    print(start_date, the_date)

    try:
        # 获取游标对象
        cur = conn.cursor()

        # 判断 Pool_std 表中是否已经存在该列
        cur.execute(f"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = '{std_table_name}'")
        columns = cur.fetchall()
        col_exists = False
        for col in columns:
            if col[0] == date_str:
                col_exists = True
                break

        # 如果 {std_table_name} 表中不存在该列，则添加该列
        if not col_exists:
            cur.execute(f"ALTER TABLE {std_table_name} ADD COLUMN `{date_str}` FLOAT DEFAULT 0.0")

        cur.execute(f"SELECT DISTINCT pool_ID FROM {apr_table_name}")
        pool_ID = cur.fetchall()

        for id in pool_ID:
            id = id[0]
            apr_mark = []

            # 查询 APR 表中的 ID，并对每个 ID 进行计算和更新
            for i in range(date_range):
                cur.execute(f"SELECT `{start_date+datetime.timedelta(days=i)}` FROM {apr_table_name} WHERE pool_ID={id}")
                day_mark = cur.fetchall()
                day_mark = day_mark [0] [0]
                #循环插入每个日期的收益情况
                #print(day_mark)
                apr_mark.append(day_mark)

            std_apr_mark = statistics.stdev(apr_mark)
            print(f'Pool#{id}标准差为{std_apr_mark}')
            cur.execute(f"UPDATE {std_table_name} SET `{date_str}`={std_apr_mark:.3f} WHERE pool_ID='{id}'")

        # 提交事务
        conn.commit()
        print('操作完成')

    except Exception as e:
        print(f"Error: {str(e)}")
        # 回滚事务
        conn.rollback()

    finally:
        # 关闭游标对象
        if cur:
            cur.close()

        # 关闭数据库连接
        if conn:
            conn.close()


end_date = datetime.date.today()
date_range = 7
update_std(end_date, date_range)