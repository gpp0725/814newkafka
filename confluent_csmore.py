import os
from concurrent.futures import ProcessPoolExecutor

import csv
from confluent_kafka import Consumer, KafkaError

import time


def conf_csm():
    c = Consumer({
        'bootstrap.servers': my_broker,
        'group.id': ci_d,
        'default.topic.config': {
            'auto.offset.reset': 'earliest',
        }
    })
    c.subscribe([table])
    co_unt = 0
    # tout = 0
    with open(path, 'a')as f_v:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                # tout += 1
                # if tout > 10:
                #     break
                continue
            co_unt += 1
            if co_unt >= 121400:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            ro_w = [msg.partition, msg.offset, msg.value]
            f_cv = csv.writer(f_v)
            f_cv.writerow(ro_w)
            # print(msg.offset, msg.value())
    c.close()
    return os.getpid(), co_unt, time.time()


if __name__ == '__main__':
    my_broker = "127.0.0.1:9092"
    ci_d = 'my_o'
    table = 'mgj2'
    path = 'confluent_kafka.csv'
    save_path = '/tmp/tmp/pycharm_project_4/814new/enhance/save/save1.csv'
    data = []
    with ProcessPoolExecutor(max_workers=2)as t_p:
        start = time.time()
        data.append(t_p.submit(conf_csm))
        data.append(t_p.submit(conf_csm))
    t_p.shutdown(wait=True)
    with open(save_path, 'a')as f:
        for d in data:
            # if d.done():
            process_id, count, end_time = d.result()
            total_time = end_time - start
            avg_time = total_time / count
            list1 = ['confluent_kafka', 'multi process single thread', total_time, avg_time]
            row = list1
            f_csv = csv.writer(f)
            f_csv.writerow(row)
            print('process {} finished in {} seconds total {} message.'.format(process_id, end_time - start, count))
    print('main id is {}'.format(os.getpid()))
