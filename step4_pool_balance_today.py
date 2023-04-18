import pymysql
import requests
import datetime
import time
from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException



# 连接到 pools.db 数据库
conn = pymysql.connect(
    host="127.0.0.1",
    user="root",
    password="xxxxxx",
    database="yourdatabase"
)
# 定义查询 API 的 URL 和请求头
substrate = SubstrateInterface(url="https://polkadot.api.onfinality.io/rpc?apikey=cc3818f8-32af-4ade-85be-ac66e9a7fad4")


table_name = 'Pool_balance'


try:
    # 获取游标对象
    cur = conn.cursor()

    # 获取当前日期，并将其格式化为字符串
    now = datetime.datetime.now()
    date_str = now.strftime("%Y-%m-%d")

    # 判断 rewards_stash_account 表中是否已经存在该列
    cur.execute(f"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = '{table_name}'")
    columns = cur.fetchall()
    col_exists = False
    for col in columns:
        if col[0] == date_str:
            col_exists = True
            break

    # 如果 {table_name} 表中不存在该列，则添加该列
    if not col_exists:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN `{date_str}` FLOAT DEFAULT 0.0")

    # 查询 {table_name} 表中的 stash_account 列的所有不同值
    cur.execute(f"SELECT DISTINCT stash_account FROM {table_name}")
    stash_accounts = cur.fetchall()
    print(stash_accounts)


    # 依次查询每个地址的余额，并将结果放置到今日的的这一列中
    for account in stash_accounts:
        print(account)
        # 查询余额
        address = account[0]
        result = substrate.query('System', 'Account', [address])
        balance = result.value['data']['free']
        balance = float(balance)/10000000000
        print(balance)
        # 更新 rewards 表中今天的这一列
        cur.execute(f"UPDATE {table_name} SET `{date_str}`={balance:.3f} WHERE stash_account='{address}'")

    # 提交更改
    conn.commit()



except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # 关闭游标对象
    cur.close()

    # 关闭数据库连接
    conn.close()