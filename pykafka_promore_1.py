import os
from concurrent.futures import ProcessPoolExecutor

import csv
from pykafka import KafkaClient
import time


def get_txt():
    with open(r'/tmp/tmp/pycharm_project_4/814new/enhance/Harry Potter.txt', 'rb')as f:
        yield from f.readlines()


def py_kafka_produce():
    count = 0
    my_broker = "127.0.0.1:9092"
    client = KafkaClient(hosts=my_broker)
    topic = client.topics[b'hh1']
    producer = topic.get_producer()
    for line in get_txt():
        if line:
            count += 1
            # print(line, count)
            producer.produce(line)
    return os.getpid(), count


def process_thread():
    with ProcessPoolExecutor(max_workers=3)as t_p:
        ob_js = []
        start = time.time()
        for i in range(3):
            data = t_p.submit(py_kafka_produce)
            ob_js.append(data)
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save2.csv', 'a')as f:
        for re in ob_js:
            t_p.shutdown(wait=True)
            if re.done():
                end_tm = time.time()
                pid, co_unt, = re.result()
                total_time = end_tm - start
                avg_time = total_time/co_unt
                list1 = ['pykafka', 'multi process single thread', total_time, avg_time]
                header = ['tools', 'With', 'total_produce_time(s)', 'avg_produce_time(s)']
                row = list1

                f_csv = csv.writer(f)
                f_csv.writerow(header)
                f_csv.writerow(row)
                print(
                    'The current process is {} ,total produce {} message ,'
                    'total time in {} seconds ,avg time in {} seconds'.format(
                        pid, co_unt, total_time, avg_time))


if __name__ == '__main__':
    process_thread()
