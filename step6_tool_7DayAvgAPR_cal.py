import pymysql
import datetime

# 连接到数据库
conn = pymysql.connect(
    host="127.0.0.1",
    user="root",
    password="1234567890",
    database="pools"
)

# 初始化时间
now = datetime.datetime.now()
date_str = now.strftime("%Y-%m-%d")

# 确定分子分母列表
apr_table_name = 'Pool_APR'
avg_table_name = 'Pool_avg'
end_date = datetime.date.today() 
print(end_date)
start_date = end_date - datetime.timedelta(days=7)
print(start_date)

try:
    # 获取游标对象
    cur = conn.cursor()

    # 判断 pool_avg 表中是否已经存在该列
    cur.execute(f"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = '{avg_table_name}'")
    columns = cur.fetchall()
    col_exists = False
    for col in columns:
        if col[0] == date_str:
            col_exists = True
            break

    # 如果 {table_name} 表中不存在该列，则添加该列
    if not col_exists:
        cur.execute(f"ALTER TABLE {avg_table_name} ADD COLUMN `{date_str}` FLOAT DEFAULT 0.0")

    # 查询 APR 表中的 ID，并对每个 ID 进行计算和更新
    print(f"SELECT pool_ID, AVG(({'+'.join([f'`{start_date + datetime.timedelta(days=i)}`' for i in range(7)])}) / 7) AS avg_earnings FROM {apr_table_name} GROUP BY pool_ID")
    cur.execute(f"SELECT pool_ID, AVG(({'+'.join([f'`{start_date + datetime.timedelta(days=i)}`' for i in range(7)])}) / 7) AS avg_earnings FROM {apr_table_name} GROUP BY pool_ID")

    

    
    rows = cur.fetchall()
    for row in rows:
        id = row[0]
        avg_result = row[1]
        cur.execute(f"UPDATE {avg_table_name} SET `{date_str}` = {avg_result:.3f} WHERE pool_ID = {id}")

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
