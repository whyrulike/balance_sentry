# !/usr/bin/env python3
import requests
import json
import time
import traceback
from loguru import logger
from datetime import datetime
from functools import wraps
from myUtils import SlackBot,RedisClient,Scheduler
import yaml
import argparse

class MySentry():
    def __init__(self, config):
        self.cf = config
        self.balance_dict = {}
        self.redis_cli = RedisClient(host=self.cf["redis"]["host"], port=self.cf["redis"]["port"], password=self.cf["redis"]["password"])
        self.slack_bot = SlackBot(token=self.cf["slack"]["slack_bot_token"], channel=self.cf["slack"]["channel_id"])


    def run_as_manager(self):
        # only for manager mode
        sentry_list = self.cf["sentry_list"]
        sentry_status = {}

        while True:

            for s in sentry_list:
                sentry_status[s] = self.redis_cli.conn.exists(s)

            logger.debug(sentry_status)
            # msg
            status_msg = "from: admin ,Sentry report: "
            for k, v in sentry_status.items():

                if v:
                    append_msg = f"{k}: online \t"
                else:
                    append_msg = f"{k}: offline! \t"
                    logger.warning("sentry: k not working")
                status_msg = status_msg + append_msg


            logger.debug(status_msg)
            self.slack_bot.send_message(status_msg)
            time.sleep(self.cf["interval_in_seconds"])

    def run(self):
        while True:

            beacon_base_url, check_range = self.cf["beacon_base_url"], self.cf["check_range"]
            step = self.cf["check_windows_size"]
            logger.debug(f"run beacon_base_url: {beacon_base_url}, check_range : {check_range}")
            self.balance_job(beacon_base_url, check_range,step)
            # keepalvie
            self.keep_sentry_alive()
            time.sleep(self.cf["interval_in_seconds"])

    # 异常输出
    def except_output(self,msg='exception information'):
        # msg define the tips from func
        def except_execute(func):
            @wraps(func)
            def execept_print(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    sign = '=' * 60 + '\n'
                    logger.debug(f'{sign}>>>异常时间：\t{datetime.now()}\n>>>异常函数：\t{func.__name__}\n>>>{msg}：\t{e}')
                    logger.debug(f'{sign}{traceback.format_exc()}{sign}')
            return execept_print
        return except_execute

    # 查询余额
    @except_output('Request Exception')
    def get_pubkey_balance(self, base_url, validator_index):
        v_balance = None
        headers = {"Content-Type": "application/json"}

        url = base_url + "/eth/v1/beacon/states/head/validators/" + str(validator_index)
        r = requests.get(url=url, headers=headers)
        if r.status_code == 200 and r.text:
            result = json.loads(r.text)
            if "data" in result:
                if "balance" in result["data"]:
                    v_balance = result["data"]["balance"]
                    # logger.debug("key:{},,,,balance:{}".format(validator_index, v_balance))
        else:
            logger.error("request url err :   {} {}".format(url,r.status_code))
            return None

        return int(v_balance)



    def keep_sentry_alive(self):
        key = self.cf["sentry_id"]
        interval_in_seconds = int(self.cf["interval_in_seconds"]) + 2*60
        self.redis_cli.conn.set(key, time.time(),ex=interval_in_seconds)


    # check the balance, if reduce，then alert
    @except_output('Check Exception')
    def check_balance(self,base_url, pubkey, key_type):
        sentry_id = self.cf["sentry_id"]
        suceess = False

        delta = None
        current_pubkey_balance = None

        current_pubkey_balance = self.get_pubkey_balance(base_url, pubkey)
        if current_pubkey_balance:
            if pubkey not in self.balance_dict:
                # first push
                self.balance_dict[pubkey] = int(current_pubkey_balance)
                logger.debug("key: {} first push".format(pubkey))
            else:
                # pubkey in balance_dict
                delta = current_pubkey_balance - self.balance_dict[pubkey]
                key_status = (pubkey, delta)
                logger.info(key_status)

                if delta < 0:
                    logger.warning("balance deduction occurred! Persistent miss behavior is possible")

                    if -3000000 <= delta < 0:

                        self.slack_bot.send_message(f"from:{sentry_id},type: {key_type} ,key: {key_status}, maybe miss attention !!!")
                        # maybe miss att
                        # asyncio.run(post_message(f"{key_status}, maybe miss attention !!!") )

                    elif -1000000000 <= delta < -3000001:
                        # maybe withdrawl
                        logger.warning(f"maybe key auto withdrawl: type: {key_type} ,key: {key_status} ")

                    else:
                        self.slack_bot.send_message(f"from:{sentry_id}, type: {key_type} ,key: {key_status},  please pay attention !!!")
                        self.slack_bot.send_message(f"from:{sentry_id}, type: {key_type} ,key: {key_status},  please pay attention !!!")


            # update balance_dict
            self.balance_dict[pubkey] = int(current_pubkey_balance)
            suceess = True
        else:
            logger.error(f" from:{sentry_id}, Key: {pubkey},Can not request balance!")

        return suceess


    @except_output('job Exception')
    def balance_job(self, base_url, check_range,step):

        for validater_set_type in check_range:
            logger.debug(f"check type: {validater_set_type}")
            # get list from redis
            # undecode_validater_set = self.redis_cli.conn.srandmember(validater_set_type, number=5)
            undecode_validater_set = self.redis_cli.conn.lrange(validater_set_type,0,-1)
            validater_set = [e.decode() for e in undecode_validater_set]

            if validater_set:
                # the import step
                check_validater_set = validater_set[::step]
                logger.debug(check_validater_set)
                logger.debug(f"len of dynamic selected check_validater is : {len(check_validater_set)}")
                for pubkey in check_validater_set:
                    self.check_balance(base_url, '0x'+pubkey, validater_set_type)
                    # beacon rpc gateway limit
                    time.sleep(0.5)

            else:
                logger.error(f"config err,{validater_set_type} not exit")


def read_yaml_config(file_path):
    # 读取yaml文件
    with open(file_path, "r") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data



if __name__ == "__main__":


    parser = argparse.ArgumentParser(description="arg list",epilog='Example usage: nohup ./balance_monitor --config config.yaml >> sentry1.log 2>&1 &\n')
    parser.add_argument("--config", help="yaml config ", required=True)
    args = parser.parse_args()
    config_file = args.config

    logger.info("init sentry ...")
    # read config from yaml

    cf = read_yaml_config(config_file)

    logger.debug(cf["mode"])
    logger.debug(cf["interval_in_seconds"])
    logger.debug(cf["beacon_base_url"])
    logger.debug(cf["check_range"])
    bot = SlackBot(token=cf["slack"]["slack_bot_token"],channel=cf["slack"]["channel_id"])
    base_url = cf["beacon_base_url"]
    check_range = cf["check_range"]

    sentry1 = MySentry(cf)
    if cf["mode"] == "manager":
        sentry1.run_as_manager()
    else:
        sentry1.run()
