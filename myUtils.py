# !/usr/bin/env python3

import redis
from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import time


class SlackBot:
    def __init__(self, token,channel):
        self.client = WebClient(token=token)
        self.channel = channel

    def send_message(self, message):
        try:
            response = self.client.chat_postMessage(
                channel=self.channel,
                text=message
            )
            logger.debug(f"alert Message posted:{response}")
        except SlackApiError as e:
            # print("Error posting message: {}".format(e))
            logger.debug(f"Error posting message: {e}")

class RedisClient:
    def __init__(self, host='127.0.0.1', port=6379, password='xxx'):
        self.host = host
        self.port = port
        self.password = password
        self.conn = None
        self.connect()

    def connect(self):
        # 连接redis服务器
        self.conn = redis.StrictRedis(host=self.host, port=self.port, password=self.password)

    def disconnect(self):
        # 断开redis服务器连接
        if self.conn:
            self.conn.connection_pool.disconnect()

    def reconnect(self, max_retry=5, retry_interval=3):
        # 重连redis服务器
        retry_count = 0
        while True:
            try:
                self.connect()
                print('Reconnect succeeded!')
                break
            except redis.exceptions.ConnectionError:
                retry_count += 1
                if retry_count >= max_retry:
                    print('Max retry count exceeded, giving up...')
                    break
                print(f'Failed to reconnect, {retry_count} retries...')
                time.sleep(retry_interval)

    def set(self, key, value):
        try:
            self.conn.set(key, value)
        except redis.exceptions.ConnectionError:
            print('Connection broken, attempting to reconnect...')
            self.reconnect()
            self.conn.set(key, value)

    def get(self, key):
        try:
            return self.conn.get(key)
        except redis.exceptions.ConnectionError:
            print('Connection broken, attempting to reconnect...')
            self.reconnect()
            return self.conn.get(key)

    def get_all(self, key):
        try:
            return self.conn.smembers(key)
        except redis.exceptions.ConnectionError:
            print('Connection broken, attempting to reconnect...')
            self.reconnect()
            return self.conn.smembers(key)

    def hash_set(self,hash_name,key,value):

        try:
            return self.conn.hset(hash_name,key,value)
        except redis.exceptions.ConnectionError:
            print('Connection broken, attempting to reconnect...')
            self.reconnect()
            return self.conn.hset(hash_name,key,value)

    def hash_get_all(self,hash_name):
        # hgetall: Return a Python dict of the hash's name/value pairs
        try:
            return self.conn.hgetall(hash_name)
        except redis.exceptions.ConnectionError:
            print('Connection broken, attempting to reconnect...')
            self.reconnect()
            return self.conn.hgetall(hash_name)
    
    def hash_get_all_key(self,hash_name):
        # hkeys: Return the list of keys within hash ``name``
        try:
            return self.conn.hkeys(hash_name)
        except redis.exceptions.ConnectionError:
            print('Connection broken, attempting to reconnect...')
            self.reconnect()
            return self.conn.hkeys(hash_name)
        
    def hash_get_len(self,hash_name):
        # hlen: Return the number of elements in hash ``name`
        try:
            return self.conn.hlen(hash_name)
        except redis.exceptions.ConnectionError:
            print('Connection broken, attempting to reconnect...')
            self.reconnect()
            return self.conn.hlen(hash_name)

    



