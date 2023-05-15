import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import rcParams
plt.tight_layout()

# importing sklearn Min Max Scaler class which is for Max Absolute Scaling
from sklearn.preprocessing import MaxAbsScaler

if __name__ == '__main__':

    # 转换为csv文件
    # movies_tmp = pd.read_table('data/movies_t.txt', sep='::', engine='python')
    # movies_tmp.to_csv('data/movies.csv')
    #
    # ratings_tmp = pd.read_table('data/ratings_t.txt', sep='::', engine='python')
    # ratings_tmp.to_csv('data/ratings.csv')
    #
    # users_tmp = pd.read_table('data/users_t.txt', sep='::', engine='python')
    # users_tmp.to_csv('data/users.csv')

    # 加上表头
    movies = pd.read_csv('data/movies.csv', names=["D", "MovieID", "Title", "Genres"])
    users = pd.read_csv('data/users.csv', names=["D", "UserID", "Gender", "Age", "Occupation", "Zip-code"])
    ratings = pd.read_csv('data/ratings.csv', names=["D", "UserID", "MovieID", "Rating", "Timestamp"])

    movies = movies.drop(["D"], axis=1)
    users = users.drop(["D"], axis=1)
    ratings = ratings.drop(["D"], axis=1)
    # print(users.sample(5))
    # print(movies.sample(5))
    # print(ratings.sample(5))

    # 查看形状
    # print(movies.shape)
    # print(users.shape)
    # print(ratings.shape)

    # 查看数据类型
    print(users.info())
    print(movies.info())
    print(ratings.info())


    # 查看是否有缺失值
    movies_missing_values_count = movies.isnull().sum()
    users_missing_values_count = users.isnull().sum()
    ratings_missing_values_count = ratings.isnull().sum()
    # print(movies_missing_values_count, users_missing_values_count, ratings_missing_values_count)

    # 设置为能显示所有的列
    pd.set_option('display.max_columns', None)

    # 合并
    data = pd.merge(users, ratings)
    data = pd.merge(data, movies)
    # print(data.sample(5))

    # dataframe输出csv文件
    outPath_movies = 'C:\\Users\\DELL\\Desktop\\data\\movies.csv'
    outPath_users = 'C:\\Users\\DELL\\Desktop\\data\\users.csv'
    outPath_ratings = 'C:\\Users\\DELL\\Desktop\\data\\ratings.csv'
    outPath_allData = 'C:\\Users\\DELL\\Desktop\\data\\allData.csv'

    movies.to_csv(outPath_movies, sep=',', index=False)
    users.to_csv(outPath_users, sep=',', index=False)
    ratings.to_csv(outPath_ratings, sep=',', index=False)
    data.to_csv(outPath_allData, sep=',', index=False)

    # 求电影评分均值
    mean_ratings = data.pivot_table(values='Rating', index='Title', columns='Gender', aggfunc='mean')
    # print(mean_ratings)

    # data_missing_values_count = data.isnull().sum()
    # print(data_missing_values_count)

    # 求不同性别受欢迎的电影, 这里评分人数要大于200
    ratings_by_title = data.groupby('Title').size()
    active_titles = ratings_by_title.index[ratings_by_title >= 200]

    mean_ratings = mean_ratings.loc[active_titles]
    # print(mean_ratings)

    # 女性
    top_female_ratings = mean_ratings.sort_values(by='F', ascending=False)
    # print(top_female_ratings)

    # 男性
    top_male_ratings = mean_ratings.sort_values(by='M', ascending=False)
    # print(top_male_ratings)

    # 最受欢迎十部
    # top_female_ratings10 = top_female_ratings['F'][:10]
    # top_male_ratings10 = top_male_ratings['M'][:10]

    # print(top_male_ratings10)

    plt.rcParams['font.sans-serif'] = ['FangSong']  # 用来正常显示中文标签
    # plt.figure(figsize=(20, 7))
    #
    # plt.barh(top_female_ratings.index[:10], top_female_ratings['F'].head(10), align='center',
    #          color='skyblue')
    # plt.gca().invert_yaxis()
    # plt.xlabel("评分")
    # plt.title("女性喜欢的电影")
    # plt.show()

    # plt.barh(top_male_ratings.index[:10], top_male_ratings['M'].head(10), align='center',
    #          color='skyblue')
    # plt.gca().invert_yaxis()
    # plt.xlabel("评分")
    # plt.title("男性喜欢的电影")
    # plt.show()

    # mean_ratings['diff'] = np.abs(mean_ratings['M'] - mean_ratings['F'])
    # sorted_by_diff = mean_ratings.sort_values(by='diff', ascending=False)[:10].copy()
    # print(sorted_by_diff)
    #
    # sorted_by_diff.plot.barh()
    #
    # plt.gcf().subplots_adjust(left=0.5, top=None, bottom=None, right=0.99)
    # plt.legend(prop={'size': 7}, loc='center')
    # plt.xlabel('评分')
    # plt.ylabel('电影名')
    # plt.title('性别差异最大的top10电影', {'fontsize': rcParams['axes.titlesize']})
    # plt.show()

    # mean_ratings['diff'] = np.abs(mean_ratings['M'] - mean_ratings['F'])
    # sorted_by_diff = mean_ratings.sort_values(by='diff', ascending=True)[:10].copy()
    # print(sorted_by_diff)
    #
    # sorted_by_diff.plot.barh()
    #
    # plt.gcf().subplots_adjust(left=0.5, top=None, bottom=None, right=0.99)
    # plt.legend(prop={'size': 7}, loc='center')
    # plt.xlabel('评分')
    # plt.ylabel('电影名')
    # plt.title('性别差异最小的top10电影', {'fontsize': rcParams['axes.titlesize']})
    # plt.show()

    # # 归一化 ###############################################################
    # # 归一化users
    # # 取出部分数据, 进行归一化，然后放回去
    # users_part = users[['Age', 'Occupation']]
    # # print(users.info())
    # ma = MaxAbsScaler()
    # users_part_new = pd.DataFrame(ma.fit_transform(users_part),
    #                          columns=users_part.columns)
    # users['Age'] = users_part_new['Age']
    # users['Occupation'] = users_part_new['Occupation']
    # # print(users)
    #
    # # 归一化ratings



