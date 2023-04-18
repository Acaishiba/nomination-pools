import mysql.connector


def create_table(table_name):
    # 连接 MySQL 数据库
    cnx = mysql.connector.connect(
      host="127.0.0.1",
      user="root",
      password="xxxxxxxxx",
      database="yourdatabse"
    )
    # 创建游标
    cursor = cnx.cursor()


    # 定义创建表格的 SQL 语句
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
      pool_ID INT PRIMARY KEY,
      status VARCHAR(255),
      metadata VARCHAR(255),
      stash_account VARCHAR(255),
      reward_account VARCHAR(255)
    )
    '''
    # 执行 SQL 语句
    cursor.execute(create_table_query)

    # 提交更改
    cnx.commit()


    # 执行查询语句
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")

    # 输出查询结果
    result = cursor.fetchone()
    if result:
        print(f"Table {table_name} has been created successfully!")
    else:
        print(f"Failed to create table {table_name}.")
    # 关闭游标和连接
    cursor.close()
    cnx.close()


create_table('Pool_rewards')
create_table('Pool_balance')
create_table('Pool_APR')
