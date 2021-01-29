# coding=utf-8
'''
Author: Ezreal
Date: 2021-01-25 11:13:52
LastEditTime: 2021-01-29 10:45:18
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /redis-in-action/chapter01.py
'''
import time
import redis


class RedisVote(object):
    def __init__(self):
        self.conn = redis.Redis(host='localhost',
                                port=6379,
                                db=0,
                                password='ezreal')
        # 一天的秒数
        self.ONE_WEEK_IN_SECONDS = 7 * 86400
        self.VOTE_SCORE = 432
        self.ARTICLES_PER_PAGE = 25

    def article_vote(self, user, article):
        cutoff = time.time() - self.ONE_WEEK_IN_SECONDS
        """zscore key member"""
        # Check to see if the article can still be voted on
        # article post time more than one week , then user cant vote for this article
        if self.conn.zscore('time:', article) < cutoff:
            return

        article_id = article.partition(':')[-1]
        # If the user hasn't voted for this article before, increment the article score and vote count
        # zincryby key increment member 给有序集合key 中的member增加increment
        # hincryby key  field_name incr_by_num 给散列中的field_name 增加incr_by_num
        if self.conn.sadd('voted:' + article_id, user):
            self.conn.zincrby('score:', self.VOTE_SCORE, article)
            self.conn.hincrby(article, 'votes', 1)

    def post_article(self, user, title, link):
        """Generate a new article id"""
        # incr key 给key增加1 incryby key amount 给key增加amount
        article_id = str(self.conn.incr("article:"))
        """Start with the posting user having voted for the article, and set the article voting information to 
        automatically expire in a week """
        # 已经投过的文章及id，将为这篇文章投过票的所有用户组成一个集合，保证不会有投重复票的情况出现，一个用户只能为这篇文章投一次票
        voted = 'voted:' + article_id
        self.conn.sadd(voted, user)
        # 投票周期为一个星期，超过一星期后将不允许投票
        self.conn.expire(voted, self.ONE_WEEK_IN_SECONDS)

        now = time.time()
        article = 'article:' + article_id
        """Create the article hash"""
        self.conn.hset(article,
                       mapping={
                           'title': title,
                           'link': link,
                           'poster': user,
                           'time': now,
                           'votes': 1,
                       })
        """ Add the article to the time and score ordered zsets"""
        self.conn.zadd('score:', {article: now + self.VOTE_SCORE})
        self.conn.zadd('time:', {article: now})

        return article_id

    def get_articles(self, page, order='score:'):
        start = (page - 1) * self.ARTICLES_PER_PAGE
        end = start + self.ARTICLES_PER_PAGE - 1

        ids = self.conn.zrevrange(order, start, end)
        articles = []
        for id in ids:
            # 获取集合score中所有的元素
            article_data = self.conn.hgetall(id)
            article_data['id'] = id
            articles.append(article_data)

        return articles

    def add_remove_groups(self, article_id, to_add=None, to_remove=None):
        if to_remove is None:
            to_remove = []

        if to_add is None:
            to_add = []

        article = 'article:' + article_id
        for group in to_add:
            self.conn.sadd('group' + group, article)

        for group in to_remove:
            self.conn.srem('group' + group, article)

    def get_group_articles(self, group, page, order='score:'):
        # Create a key for each group and each sort order
        key = order + group  # destination of the specific set while excute zinterstore
        # If we haven't sorted these articles recently, we should sort them
        if not self.conn.exists(key):
            # Actually sort the articles in the group based on score or recency
            # 使用 AGGREGATE 选项，你可以指定并集的结果集的聚合方式。
            # 默认使用的参数 SUM ，可以将所有集合中某个成员的 score 值之 和 作为结果集中该成员的 score 值；使用参数 MIN ，可以将所有集合中某个成员的 最小 score 值作为结果集中该成员的 score 值；
            # 而参数 MAX 则是将所有集合中某个成员的 最大 score 值作为结果集中该成员的 score 值
            self.conn.zinterstore(key, ['group:' + group, order],
                                  aggregate='max')

            self.conn.expire(key, 60)
            return self.get_articles(page, key)


if __name__ == '__main__':
    redisvote = RedisVote()
    article_id = redisvote.post_article('username', 'A title',
                                        'http://www.baidu.com')
    print("We posted a new article with id:", article_id)
    print('\n')

    print("Its HASH looks like:")
    r = redisvote.conn.hgetall('article:' + article_id)
    print(r)
    print('\n')

    redisvote.article_vote('hasagi', 'article:' + article_id)
    print("We voted for the article, it now has votes:")
    v = int(redisvote.conn.hget('article:' + article_id, 'votes'))
    print(v)
    print('\n')

    print("Its HASH looks like:")
    r = redisvote.conn.hgetall('article:' + article_id)
    print(r)
    print('\n')

    redisvote.article_vote('yasuo', 'article:' + article_id)
    print("We voted for the article, it now has votes:")
    v = int(redisvote.conn.hget('article:' + article_id, 'votes'))
    print(v)
    print('\n')

    print("Its HASH looks like:")
    r = redisvote.conn.hgetall('article:' + article_id)
    print(r)
    print('\n')

    print("The currently highest-scoring articles are:")
    # 第一页的中分数值最大的是
    articles = redisvote.get_articles(1)
    print articles
    print('\n')

    redisvote.add_remove_groups(article_id, ['new-group'])
    print("We added the article to a new group, other articles include:")
    articles = redisvote.get_group_articles('new-group', 1)
    print articles

    to_del = (
                redisvote.conn.keys('time:*') + redisvote.conn.keys('voted:*') + redisvote.conn.keys('score:*') +
                redisvote.conn.keys('article:*') + redisvote.conn.keys('group:*')
        )

    if to_del:
        redisvote.conn.delete(*to_del)
