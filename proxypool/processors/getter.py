from loguru import logger
from proxypool.storages.redis import RedisClient
from proxypool.setting import PROXY_NUMBER_MAX
from proxypool.crawlers import __all__ as crawlers_cls


class Getter(object):
    """
    获取代理池
    """
    
    def __init__(self):
        """
        初始化 db 和 爬虫
        """
        self.redis = RedisClient()
        self.crawlers_cls = crawlers_cls
        self.crawlers = [crawler_cls() for crawler_cls in self.crawlers_cls]
    
    def is_full(self):
        """
        判断代理池是否已经满了
        return: bool
        """
        return self.redis.count() >= PROXY_NUMBER_MAX
    
    @logger.catch
    def run(self):
        """
        运行代理抓取工具
        :return:
        """
        if self.is_full():
            return
        for crawler in self.crawlers:
            logger.info(f'爬取 {crawler} to get proxy')
            for proxy in crawler.crawl():
                self.redis.add(proxy)


if __name__ == '__main__':
    getter = Getter()
    getter.run()
