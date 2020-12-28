import asyncio
import aiohttp
from loguru import logger
from proxypool.schemas import Proxy
from proxypool.storages.redis import RedisClient
from proxypool.setting import TEST_TIMEOUT, TEST_BATCH, TEST_URL, TEST_VALID_STATUS, TEST_ANONYMOUS, TEST_ANONYMOUS_MYSELF
from aiohttp import ClientProxyConnectionError, ServerDisconnectedError, ClientOSError, ClientHttpProxyError
from asyncio import TimeoutError
from proxypool.utils.proxy import is_valid_proxy, convert_proxy_or_proxies

EXCEPTIONS = (
    ClientProxyConnectionError,
    ConnectionRefusedError,
    TimeoutError,
    ServerDisconnectedError,
    ClientOSError,
    ClientHttpProxyError,
    AssertionError
)


class Tester(object):
    """
    测试队列中代理 的 测试器
    """
    
    def __init__(self):
        """
        初始化redis
        """
        self.redis = RedisClient()
        self.loop = asyncio.get_event_loop()

    # 测试匿名1
    async def test_anonymous1(self, proxy: Proxy, session):

        url = 'https://httpbin.org/ip'
        async with session.get(url, timeout=TEST_TIMEOUT) as response:
            resp_json = await response.json()
            origin_ip = resp_json['origin']
        async with session.get(url, proxy=f'http://{proxy.string()}', timeout=TEST_TIMEOUT) as response:
            resp_json = await response.json()
            anonymous_ip = resp_json['origin']

        logger.debug(f'只测试匿名代理： {origin_ip != anonymous_ip} -- 结果匿名ip：{anonymous_ip}，代理ip：{proxy.string()}')
        assert origin_ip != anonymous_ip
        assert proxy.host == anonymous_ip

    # 测试匿名2
    async def test_anonymous2(self, proxy: Proxy, session):
        url = 'http://km.chik.cn/ip'
        async with session.get(url, timeout=TEST_TIMEOUT) as response:
            resp_json = await response.json()
            origin_ip = resp_json['origin']
        async with session.get(url, proxy=f'http://{proxy.string()}', timeout=TEST_TIMEOUT) as response:
            resp_json = await response.json()
            anonymous_ip = resp_json['origin']

        logger.debug(f'只测试匿名代理2： {origin_ip != anonymous_ip} -- 结果匿名ip：{anonymous_ip}，代理ip：{proxy.string()}')
        assert origin_ip != anonymous_ip
        assert proxy.host == anonymous_ip

    async def test(self, proxy: Proxy):
        """
        测试单个代理
        :param proxy: 代理对象
        :return:
        """
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            try:
                # logger.debug(f'测试代理ip为： {proxy.string()}')

                # 如果 TEST_ANONYMOUS 设置为 True, 确保
                # 代理具有隐藏真实IP的作用
                # TEST_ANONYMOUS 测试匿名
                if TEST_ANONYMOUS:
                    await self.test_anonymous1(proxy, session)
                if TEST_ANONYMOUS_MYSELF:
                    await self.test_anonymous2(proxy, session)

                async with session.get(TEST_URL, proxy=f'http://{proxy.string()}', timeout=TEST_TIMEOUT,
                                       allow_redirects=False) as response:
                    if response.status in TEST_VALID_STATUS:
                        self.redis.max(proxy)
                        logger.debug(f'代理 {proxy.string()} 是有效的, 设置最大分数')
                    else:
                        self.redis.decrease(proxy)
                        logger.debug(f'代理 {proxy.string()} 不可使用, 降低分数')

            except EXCEPTIONS:
                self.redis.decrease(proxy, score=-10)
                # logger.error(f'代理 {proxy.string()} 异常, 降低分数')


    @logger.catch
    def run(self):
        """
        主测试程序
        :return:
        """
        # event loop of aiohttp 的事件循环
        logger.info('开始测试...')

        # proxies = convert_proxy_or_proxies("191.235.98.23:3128")
        # [self.test(proxy) for proxy in proxies]

        count = self.redis.count()
        logger.debug(f'共 {count} 个代理等待测试')
        cursor = 0
        while True:
            logger.debug(f'测试代理游标 {cursor}, count {TEST_BATCH}')
            cursor, proxies = self.redis.batch(cursor, count=TEST_BATCH)
            if proxies:
                # proxies = convert_proxy_or_proxies("191.235.98.23:3128")
                tasks = [self.test(proxy) for proxy in proxies]
                self.loop.run_until_complete(asyncio.wait(tasks))
            if not cursor:
                break


if __name__ == '__main__':
    tester = Tester()
    tester.run()
