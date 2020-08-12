import platform
from os.path import dirname, abspath, join
from environs import Env
from loguru import logger
from proxypool.utils.parse import parse_redis_connection_string


env = Env()
env.read_env()

# definition of flags
IS_WINDOWS = platform.system().lower() == 'windows'

# 系统根目录
ROOT_DIR = dirname(dirname(abspath(__file__)))
LOG_DIR = join(ROOT_DIR, env.str('LOG_DIR', 'logs'))

# definition of environments
DEV_MODE, TEST_MODE, PROD_MODE = 'dev', 'test', 'prod'
APP_ENV = env.str('APP_ENV', DEV_MODE).lower()
APP_DEBUG = env.bool('APP_DEBUG', True if APP_ENV == DEV_MODE else False)
APP_DEV = IS_DEV = APP_ENV == DEV_MODE
APP_PROD = IS_PROD = APP_ENV == PROD_MODE
APP_TEST = IS_TEST = APP_ENV == TEST_MODE




# redis host
REDIS_HOST = env.str('REDIS_HOST', '127.0.0.1')
REDIS_PORT = env.int('REDIS_PORT', 6379)
REDIS_DB = env.int('REDIS_DB', 0)
# redis password, 如果没有密码请设置为 None
REDIS_PASSWORD = env.str('REDIS_PASSWORD', 'Jiazhiyi263')
# redis connection string, 链接字符串：例如， redis://[password]@host:port 或 rediss://[password]@host:port/0
REDIS_CONNECTION_STRING = env.str('REDIS_CONNECTION_STRING', None)

if REDIS_CONNECTION_STRING:
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB = parse_redis_connection_string(REDIS_CONNECTION_STRING)

# redis hash table key name
REDIS_KEY = env.str('REDIS_KEY', 'proxies:universal')




# 代理池分数设置
PROXY_SCORE_MAX = 100
PROXY_SCORE_MIN = 0
PROXY_SCORE_INIT = 20
PROXY_SCORE_SUB = 10

# 代理池最大最小数量
PROXY_NUMBER_MAX = 500
PROXY_NUMBER_MIN = 0



# 定义 TESTER 执行周期, 每 CYCLE_TESTER 秒执行一次
CYCLE_TESTER = env.int('CYCLE_TESTER', 20)
# 定义 GETTER 执行周期, 每 CYCLE_GETTER 秒执行一次
CYCLE_GETTER = env.int('CYCLE_GETTER', 100)
# 定义 GET 方法的请求超时时长，单位秒
GET_TIMEOUT = env.int('GET_TIMEOUT', 10)

# 定义 测试URL 以该URL来测试连通率，进行加减分数
TEST_URL = env.str('TEST_URL', 'https://www.baidu.com')
TEST_TIMEOUT = env.int('TEST_TIMEOUT', 10)
TEST_BATCH = env.int('TEST_BATCH', 20)

# 是否只保存匿名的代理
TEST_ANONYMOUS = True
# TEST_HEADERS = env.json('TEST_HEADERS', {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
# })
TEST_VALID_STATUS = env.list('TEST_VALID_STATUS', [200, 206, 302])

# 定义 api
API_HOST = env.str('API_HOST', '0.0.0.0')
API_PORT = env.int('API_PORT', 5555)
API_THREADED = env.bool('API_THREADED', True)

# flags of enable
ENABLE_TESTER = env.bool('ENABLE_TESTER', True)
ENABLE_GETTER = env.bool('ENABLE_GETTER', True)
ENABLE_SERVER = env.bool('ENABLE_SERVER', True)

logger.add(env.str('LOG_RUNTIME_FILE', join(LOG_DIR, 'runtime.log')), level='DEBUG', rotation='1 week', retention='20 days')
logger.add(env.str('LOG_ERROR_FILE', join(LOG_DIR, 'error.log')), level='ERROR', rotation='1 week')

