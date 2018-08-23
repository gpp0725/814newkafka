import os
from concurrent.futures import ProcessPoolExecutor

import csv
from confluent_kafka import Consumer, KafkaError

import time


def conf_csm(ser_ver, i_d, early, table):
    c = Consumer({
        'bootstrap.servers': ser_ver,
        'group.id': i_d,
        'default.topic.config': {
            'auto.offset.reset': early,
            # 'consumer_timeout_ms': float('inf'),
            # 'session.timeout.ms': 3000
        }
    })
    c.subscribe([table])
    co_unt = 0
    tout = 0
    with open('confluent_kafkamore.csv', 'a')as f_v:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                tout += 1
                if tout > 10:
                    break
                continue
            co_unt += 1
            # if count >= 14069:
            #     break
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
    data = []
    with ProcessPoolExecutor(max_workers=2)as t_p:
        start = time.time()
        data.append(t_p.submit(conf_csm, ser_ver=my_broker, i_d='sui_b', early='earliest', table='hh1', ))
        data.append(t_p.submit(conf_csm, ser_ver=my_broker, i_d='sui_b', early='earliest', table='hh1', ))
    t_p.shutdown(wait=True)
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save1.csv', 'a')as f:
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

# no_mal
