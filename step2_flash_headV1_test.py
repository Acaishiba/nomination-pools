import mysql.connector
import requests
import time

def flash_table_head(table_name):
    # 连接 MySQL 数据库
    cnx = mysql.connector.connect(
      host="127.0.0.1",
      user="root",
      password="xxxxx",
      database="database"
    )

    # 定义查询 API 的 URL 和请求头
    url = 'https://polkadot.api.subscan.io/api/scan/nomination_pool/pools'
    # 查询详细信息的url为url2
    url2 = 'https://polkadot.api.subscan.io/api/scan/nomination_pool/pool'

    headers = {
        'Content-Type': 'application/json',
        'X-API-Key': 'xxxxxxxxxxxxxyour api key'
    }

    params = {}

    #开始获取总矿池数
    response_pools_count = requests.post(url, headers=headers, json=params)

    if response_pools_count.status_code == 200:
        pools_count = response_pools_count.json()["data"]["count"]
        # 处理查询结果
        print(pools_count)
    else:
        print(f"请求失败，HTTP状态码：{response_pools_count.status_code}")


    cur = cnx.cursor()

    cur.execute(f"SELECT MAX(pool_ID) FROM {table_name}")
    result = cur.fetchone()
    max_pool_id = result[0]
    print(max_pool_id)

    if max_pool_id == None:
        cur.execute(f"INSERT INTO {table_name} (pool_ID) VALUES (1)")
        max_pool_id = 1


    # 获取新的 pool_ID 值
    new_pool_id = pools_count

    # 如果原最大值大于等于新值，则不进行修改
    if max_pool_id >= new_pool_id:
        print("原最大值大于等于新值，无需修改。")
    else:
        # 计算需要递增的数值
        increment = new_pool_id - max_pool_id
        print('new is',new_pool_id)
        print('old is',max_pool_id)

        print(increment)

        # 插入新记录
        for i in range(max_pool_id + 1, new_pool_id + 1):
            sql = f"INSERT INTO {table_name} (pool_ID) VALUES (%s)"
            cur.execute(sql, (i,))
            print(f"已成功插入 pool_ID 为 {i} 的记录。")

        # 提交更改并关闭数据库连接
        cnx.commit()
        print("已成功插入新记录。")
    cur.close()



    try:
        # 获取游标对象
        cur = cnx.cursor()

        #选择pool_ID列
        cur.execute(f"SELECT DISTINCT pool_ID FROM {table_name}")
        pool_ID = cur.fetchall()
        print(pool_ID)

        for poolid in pool_ID: 
            poolid_count = int(poolid[0])
            response_state = requests.post(url2, headers=headers, json={"pool_id": poolid_count})
            print(poolid_count)
            print(response_state.json())

            if response_state.json()["code"] == 0:
            #获取各个rpc数据
                state = response_state.json()['data']['state']       
                metadata = response_state.json()['data']['metadata']
                stash_account = response_state.json()['data']['pool_account']['address']
                reward_account = response_state.json()['data']['pool_reward_account']['address']

                print(state)
                print(metadata)
                print(stash_account)
                print(reward_account)

                #将数据根据poolid刷新进表
                update_query = f"UPDATE {table_name} SET status = %s, metadata = %s, stash_account = %s, reward_account = %s WHERE pool_ID = %s"
                cur.execute(update_query, (state, metadata, stash_account, reward_account, poolid_count))
                print("-----------Flashing------------")
                time.sleep(0.21)

            else:
                print(f"Error encountered with pool ID {poolid_count}. Exiting loop.")
                print("-----------Flashing------------")
                time.sleep(0.21)
                continue

        # 提交事务
        cnx.commit()

    except Exception as e:
        print(f"Error: {str(e)}")
        # 回滚事务
        cnx.rollback()

    finally:
        # 关闭游标对象
        cur.close()

        # 关闭数据库连接
        cnx.close()
    print('执行完成')

flash_table_head('Pool_rewards')
flash_table_head('Pool_balance')
flash_table_head('Pool_APR')