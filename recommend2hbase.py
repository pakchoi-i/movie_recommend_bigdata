"""
    将推荐结果传入hbase中
"""
import happybase
import pandas as pd
from pyhdfs import HdfsClient

pool = happybase.ConnectionPool(size=3, host='192.168.237.100', port=9090, protocol='compact', transport='framed')

# 从hdfs中加载电影信息
print('loading movie information...')
item_col = ['movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action',
            'Adventure', 'Animation', "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy',
            'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
client = HdfsClient('192.168.237.100:50070', user_name='hadoop')
item_file = client.open('/movieLens/ml-100k/u.item')
item_content = str(item_file.read(), 'ISO-8859-1')
with open('item.csv', 'w', encoding='ISO-8859-1') as local_f:
    local_f.write(item_content)
item = pd.read_table('item.csv', encoding='ISO-8859-1', sep='|',
                     header=None, names=item_col, parse_dates=['release_date', 'video_release_date']
                     )

print('connecting...')
# 连接hbase
with pool.connection() as conn:
    print('creating table....')
    # 在hbase中创建推荐信息表
    if 'predicts'.encode('utf-8') not in conn.tables():
        conn.create_table('predicts',
                          {
                              'user': dict(),
                              'recommend_movies': dict()
                          })
    # 启用predicts表
    elif not conn.is_table_enabled('predicts'):
        conn.enable_table('predicts')
    print('connecting table...')
    table = conn.table('predicts')

    with table.batch(batch_size=10) as bat:
        # 将离线推荐结果上传到hbase中
        print('uploading result...')
        predicts_file = open('ml-100k/predicts.csv', encoding='utf-8')
        df = pd.read_csv(predicts_file)
        for i in range(len(df)):
            # print(i)
        # for i in range(5):
            # print(len(df.loc[i]))
            table_dict = {}
            table_dict['user:id'] = str(int(df.loc[i]['user_id']))
            # 选取前10个推荐结果
            for idx in range(0, 10):
                idx_str = str(idx)
                movie_id = int(df.loc[i][idx_str])
                movie_title = item.loc[movie_id - 1]['movie_title']
                # print(movie_title)
                table_dict['recommend_movies:{}'.format(idx + 1)] = movie_title
            # print(table_dict)
            bat.put('row{}'.format(i+1), table_dict)
