import os

import csv
from confluent_kafka import Producer
import codecs
import time
from concurrent.futures import ProcessPoolExecutor


def get_txt():
    with codecs.open(r'/tmp/tmp/pycharm_project_4/814new/enhance/Harry Potter.txt', 'rb')as f:
        yield from f.readlines()


def pro_duc():
    c = Producer({
        'bootstrap.servers': my_broker,
        'group.id': my_group,
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    })
    count = 0
    for line_data in get_txt():
        if line_data:
            count += 1
            # print(line_data.decode())
            c.produce(table, line_data)
    c.flush()

    return os.getpid(), count


def process_thread():
    with ProcessPoolExecutor(max_workers=3)as t_p:  # multi_processes
        ob_js = []
        start = time.time()
        for i in range(3):
            data = t_p.submit(pro_duc)
            ob_js.append(data)
    with open(save_time_data, 'a')as f:
        for re in ob_js:
            t_p.shutdown(wait=True)
            if re.done():
                end_tm = time.time()
                pid, co_unt, = re.result()
                total_time = end_tm - start
                avg_time = total_time / co_unt
                list1 = ['confluent_kafka', 'multi process single thread', total_time, avg_time]
                row = list1
                f_csv = csv.writer(f)
                f_csv.writerow(row)
                print(
                    'The current process is {} ,total produce {} message ,'
                    'total time in {} seconds ,avg time in {} seconds'.format(
                        pid, co_unt, total_time, avg_time))
        # print(re.result(), end_tm - start)


if __name__ == '__main__':
    my_broker = "127.0.0.1:9092"
    my_group = 'my_group'
    table = 'hh1'
    save_time_data = '/tmp/tmp/pycharm_project_4/814new/enhance/save/save2.csv'
    process_thread()
