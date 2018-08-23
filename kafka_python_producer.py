# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2018/8/17 15:35
# @Author   : Sz
# @File     : kafka_python_producer.py
import os
import time
from concurrent.futures import ProcessPoolExecutor

import csv
from kafka import KafkaProducer


def get_text():
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/Harry Potter.txt', 'rb') as f:
        yield from f.readlines()


def produce_():
    producer = KafkaProducer(bootstrap_servers=[server])
    co_unt = 0
    for content in get_text():
        if content:
            co_unt += 1
            # print(content.decode())
            # print(content)
            producer.send('fr2', content)
    return co_unt, os.getpid()


def process_thread():
    with ProcessPoolExecutor(max_workers=3)as t_p:
        ob_js = []
        start = time.time()
        for i in range(3):
            data = t_p.submit(produce_)
            ob_js.append(data)
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save2.csv', 'a')as f:
        for re in ob_js:
            t_p.shutdown(wait=True)
            if re.done():
                end_tm = time.time()
                co_unt, pid = re.result()
                total_time = end_tm - start
                avg_time = total_time / co_unt
                list1 = ['pykafka', 'multi process single thread', total_time, avg_time]
                row = list1

                f_csv = csv.writer(f)
                f_csv.writerow(row)
                print(
                    'The current process is {} ,total produce {} message ,'
                    'total time in {} seconds ,avg time in {} seconds'.format(
                        pid, co_unt, total_time, avg_time))


if __name__ == '__main__':
    server = '127.0.0.1:9092'
    print('start')
    start_time = time.time()
    process_thread()
    print('finish!')
    end_time = time.time()
    print(end_time - start_time)  # 7.482218027114868

    # x = kpp.get_text()
    # for content in x:
    #     print(content)

