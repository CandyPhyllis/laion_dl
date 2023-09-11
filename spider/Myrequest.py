# -*- coding:utf-8 -*-
import requests
from my_logger import Loggings

import warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

logger = Loggings()
MAX_REQUEST = 5


def request(method, url, **kwargs):
    """封装requests
    method : get 或者 post
    url : 要请求的网址
    ip_host :
    """
    # 自己给做ip代理
    timeout = 6
    request_method = {'get': requests.get, 'post': requests.post}
    count = MAX_REQUEST
    proxy = {"http": f"http://127.0.0.1:7890",
             "https": f"http://127.0.0.1:7890"}
    while count > 0:
        try:
            response = request_method[method](url, timeout=timeout, proxies=proxy, **kwargs)
            if response.status_code == 200:
                logger.info(f"suceess!")
                return response
            else:
                print(f"请求状态码错误（重新请求）：{response.status_code}-{response.url}")
        except Exception as e:
            import traceback
            logger.error(f"请求报错原因：{e}")
        count = count - 1


if __name__ == '__main__':
    pass
