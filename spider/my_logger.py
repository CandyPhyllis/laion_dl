# -*- coding:utf-8 -*-
import os
import time
from loguru import logger
from pathlib import Path

LOG_PATH = Path(Path.cwd().parent, "logs")

class Loggings:
    """
    日志类
    """
    __instance = None
    # 配置日志路径，大小，编发方式
    if not os.path.exists(LOG_PATH):
        os.mkdir(LOG_PATH)
    logger.add(
        # 日志文件路径
        f"{LOG_PATH}/{time.strftime('%Y_%m_%d')}.log",
        # 每天0生成一个日志文件
        rotation="00:00",
        # 日志编码
        encoding="utf-8",
        # 是否异步写入
        enqueue=True,
      )

    def __new__(cls, *args, **kwargs):
        """单例模式"""
        if not cls.__instance:
            cls.__instance = super(Loggings, cls).__new__(cls, *args, **kwargs)
        return cls.__instance

    def info(self, msg):
        """
        记录INFO级别日志
        :param msg: str
        :return:
        """
        return logger.info(msg)

    def debug(self, msg):
        """
        记录DEBUG级别日志
        :param msg: str
        :return:
        """
        return logger.debug(msg)

    def warning(self, msg):
        """
        记录WARNING级别日志
        :param msg: str
        :return:
        """
        return logger.warning(msg)

    def error(self, msg):
        """
        记录ERROR级别日志
        :param msg: str
        :return:
        """
        return logger.error(msg)
