"""
    与用户交互，展示hbase中的推荐结果
"""
import happybase

pool = happybase.ConnectionPool(size=3, host='192.168.237.100', port=9090,
                                protocol='compact', transport='framed')
# 连接hbase
with pool.connection() as conn:
    table = conn.table('predicts')
    while True:
        # 获取用户输入
        target_id = int(input('请输入需要查询的用户id：\n'))
        # 找到hbase中的结果
        target_row = table.row('row{}'.format(target_id))
        # 取出推荐的电影
        movie_list = []
        curr_num = 0
        for key in target_row.keys():
            key_str = key.decode('utf-8')   # bytes转化为string
            if 'user' in key_str:
                continue
            curr_rank = int(key_str.split(':')[1])
            movie_list.append((curr_rank, target_row[key].decode('utf-8')))
        # 按推荐程度，对返回的推荐结果进行重新排序
        movie_list = sorted(movie_list, key=lambda x: x[0])
        # 输出结果
        print(f'用户{target_id}的电影推荐结果为（按推荐程度降序）：')
        print('-------------------------------------------------------------------------------------')
        for rank, movie_title in movie_list:
            print(f'\t{rank}\t|\t{movie_title}')
        print('-------------------------------------------------------------------------------------\n')
