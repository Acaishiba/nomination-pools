import pymysql
import requests
import datetime
import time

# 连接到 pools.db 数据库
conn = pymysql.connect(
    host="127.0.0.1",
    user="xxxyourusernamexx",
    password="xxyourpasswordxxxxx",
    database="yourdatabase"
)
# 定义查询 API 的 URL 和请求头
url = 'https://polkadot.api.subscan.io/api/scan/account/reward_slash'
headers = {'Content-Type': 'application/json', 'X-API-Key': 'xxxxxxxxyourapikeyxxxxx'}
#row_page = {'row': 5, 'page': 0}
#payload = {'row': 5, 'page': 0, 'address': '13UVJyLnbVp8c4FQeiGZuEPKfanQpr45nvH5LmUqKKqjNQSy'}


table_name = 'Pool_rewards'

try:
    # 获取游标对象
    cur = conn.cursor()

    # 获取当前日期，并将其格式化为字符串
    now = datetime.datetime.now()
    date_str = now.strftime("%Y-%m-%d")

    # 判断 rewards 表中是否已经存在该列
    cur.execute(f"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = '{table_name}'")
    columns = cur.fetchall()
    col_exists = False
    for col in columns:
        if col[0] == date_str:
            col_exists = True
            break

    # 如果 rewards 表中不存在该列，则添加该列
    if not col_exists:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN `{date_str}` FLOAT DEFAULT 0.0")

    # 查询 rewards 表中的 reward_account 列的所有不同值
    cur.execute(f"SELECT DISTINCT reward_account FROM {table_name}")
    reward_accounts = cur.fetchall()
    print(reward_accounts)

    # 依次查询每个地址的收益，并将结果累加到今日的收入中
    for account in reward_accounts:
        # 使用 API 查询收益信息
        print(account)
        payload = {'row': 5, 'page': 0, 'address': account[0]}
        response = requests.post(url, headers=headers, json=payload)
        
        print(response)
        #判断是否查询成功
        if response.status_code == 200:
            result = response.json()
            #print(result)
            if result is not None and result["code"] == 0 and result["data"]["count"] != 0:
                
                amount = float(0)
                for item in result["data"]["list"]:
                    timestamp = item["block_timestamp"]
                    dt = datetime.datetime.fromtimestamp(timestamp)
                    if dt.date() == now.date():
                        amount += float(item["amount"]) / float(10000000000)
                print(amount)
                cur.execute(f"UPDATE {table_name} SET `{date_str}` = {amount:.3f} WHERE reward_account = '{account[0]}'")
            elif result["data"]["count"] == 0:
                amount = float(0)
                print('no result,the rewards is 0')
                print(amount)
                # 更新今日的收入到 rewards 表中
                cur.execute(f"UPDATE {table_name} SET `{date_str}` = {amount:.3f} WHERE reward_account = '{account[0]}'")
            print('------------------')
            time.sleep(0.5)
            
                

    # 提交事务
    conn.commit()

except Exception as e:
    # 回滚事务
    conn.rollback()
    print(f"Error: {e}")

finally:
    # 关闭连接
    conn.close()
