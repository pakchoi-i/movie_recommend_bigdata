"""
    对所有用户生成推荐结果，并传入hdfs中
"""
import pandas as pd
import numpy as np
from pyhdfs import HdfsClient

##通过物品同现矩阵*用户评分矩阵=推荐结果，这里是指定一个用户来生成对应的用户的推荐结果。
def get_itemCF(item_matrix, user_score, col_name):
    """
    item_matrix: 物品同现矩阵，DataFrame类型
    user_score:  用户评分矩阵，DataFrame类型,某一个指定的用户的评分矩阵
    return:      用户对对应的物品的兴趣值 得到的类型为DataFrame类型，
    """
    columns = item_matrix.columns
    # 对列名进行重新排序，按照物品同现矩阵的列名进行排序
    user_score = user_score[columns]
    # 过滤掉用户曾经看过的电影
    user_movie = user_score[user_score.values == 0].index

    # item_matrix=np.mat(item_matrix.as_matrix(columns=None))
    # user_score=np.mat(user_score.as_matrix(columns=None)).T
    item_matrix = np.mat(item_matrix.to_numpy())
    user_score = np.mat(user_score.to_numpy()).T
    result_score = item_matrix * user_score
    result = pd.DataFrame(result_score, index=columns, columns=['rating'])
    result = result.sort_values(by='rating', ascending=False)
    result[col_name] = columns
    return result[result[col_name].isin(user_movie)]


item_matrix = pd.read_csv('ml-100k/user_matrix.csv', index_col=0)
user_item_matrix = pd.read_csv('ml-100k/user_item_matrix.csv', index_col=0)
# print(item_matrix)
# print(user_item_matrix)
# user_result = get_itemCF(item_matrix, user_item_matrix.loc[196, :], 'movie_id')
# print(user_result)
user_result = []
for i in range(user_item_matrix.shape[0]):
    user_result.append(get_itemCF(item_matrix, user_item_matrix.loc[i + 1, :], 'movie_id')['movie_id'].reset_index(drop=True))
    # user_result_info.append(get_itemCF(item_matrix, user_item_matrix.loc[i,:],'movie_id').merge(item, how='inner', left_on='movie_id', right_on='movie_id', copy=False))


assert len(user_result) == user_item_matrix.shape[0]
# 输出结果
res_pd = user_result[0]
for i in range(1, len(user_result)):
    res_pd = pd.concat([res_pd, user_result[i]], axis=1)
res_pd = res_pd.T.reset_index(drop=True)
# 添加用户列
res_pd.insert(0, 'user_id', range(1, len(res_pd)+1))
res_pd.to_csv('ml-100k/predicts.csv', index=False)
print(res_pd)

# 将结果存入hdfs
client = HdfsClient('192.168.237.100:50070', user_name='hadoop')
client.copy_from_local('ml-100k/predicts.csv', '/movieLens/ml-100k/predicts.csv')
