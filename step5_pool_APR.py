import pymysql
import datetime
import time

# 连接到数据库
conn = pymysql.connect(
    host="127.0.0.1",
    user="root",
    password="xxxxxxx",
    database="yourdatabase"
)
# 初始化时间
now = datetime.datetime.now()
date_str = now.strftime("%Y-%m-%d")
print(date_str)

#确定分子分母列表
Numerator_table_name = 'Pool_rewards'
Denominator_table_name = 'Pool_balance'
Ratio_table_name = 'Pool_APR'

try:
    # 获取游标对象
    cur = conn.cursor()

# 获取当前日期，并将其格式化为字符串
    now = datetime.datetime.now()
    date_str = now.strftime("%Y-%m-%d")

    # 判断 rewards_stash_account 表中是否已经存在该列
    cur.execute(f"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = '{Ratio_table_name}'")
    columns = cur.fetchall()
    col_exists = False
    for col in columns:
        if col[0] == date_str:
            col_exists = True
            break

    # 如果 {table_name} 表中不存在该列，则添加该列
    if not col_exists:
        cur.execute(f"ALTER TABLE {Ratio_table_name} ADD COLUMN `{date_str}` FLOAT DEFAULT 0.0")


    # 获取 rewards 表中的所有池子ID和日期组合的列表
    cur.execute(f"SELECT DISTINCT pool_ID, `{date_str}` FROM {Numerator_table_name}")
    pool_dates = cur.fetchall()
    print(pool_dates)

    # 遍历池子ID和日期组合列表，计算 rewards_stash_account 表中相同池子ID和日期的数据除以 rewards 表中相同池子ID和日期的数据
    for pool_date in pool_dates:
        pool_id = pool_date[0]
        date = float(pool_date[1])
        print('Pool ID is: ',pool_id)
        print('Rewards is :',date)
        rewards_result = date


        # 查询 rewards 表中池子ID和日期对应的数据
        #cur.execute(f"SELECT `{date_str}` FROM rewards WHERE pool_ID='{pool_id}' AND `{date_str}`='{date}'")
        #rewards_result = cur.fetchone()
        #print(rewards_result)
        #if rewards_result is None:
        #    continue
        #rewards_value = rewards_result[0]

        # 查询 rewards_stash_account 表中池子ID和日期对应的数据
        cur.execute(f"SELECT `{date_str}` FROM {Denominator_table_name} WHERE pool_ID='{pool_id}'")
        rewards_stash_account_result = cur.fetchone()
        rewards_stash_account_result = float(rewards_stash_account_result[0])

        print('Pool balance is :',rewards_stash_account_result)

        # 计算 rewards 表中的值除以 rewards_stash_account 表中的值
        if rewards_stash_account_result != 0:
            ratio = (rewards_result / rewards_stash_account_result)*365
            print('Today`s APY is :', f"{ratio*100:.4f}%") 
        else:
            ratio = 0.000000
            print('balance is 0') 

        # 将计算得到的比率更新到 rewards_ratio 表中
        cur.execute(f"UPDATE {Ratio_table_name} SET `{date_str}`={ratio:.6f} WHERE pool_id='{pool_id}'")
        
        print("-------------------------------------")

    # 提交事务
    conn.commit()
    

except Exception as e:
    print(f"Error: {str(e)}")
    # 回滚事务
    conn.rollback()
finally:
    # 关闭游标对象
    cur.close()

    # 关闭数据库连接
    conn.close()
