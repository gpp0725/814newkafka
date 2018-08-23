import time
from concurrent.futures import ProcessPoolExecutor

import csv
from kafka import KafkaConsumer
import os


def get_consume(table, i_d, ser_ver, early):
    consumer = KafkaConsumer(table,
                             group_id=i_d,
                             bootstrap_servers=ser_ver,
                             auto_offset_reset=early,
                             enable_auto_commit=False,
                             consumer_timeout_ms=2000
                             )
    count = 0
    with open('kafkamore.csv', 'a')as f:
        for message in consumer:
            count += 1
            # print(os.getpid(), message.topic, message.partition, message.offset, message.key,
            #       message.value.decode('utf-8'))
            row = [message.partition, message.offset, message.value]
            f_cv = csv.writer(f)
            f_cv.writerow(row)

    return os.getpid(), count, time.time()


def mul_thread():
    data = []
    with ProcessPoolExecutor(max_workers=2)as t_p:
        start = time.time()
        data.append(t_p.submit(get_consume, table='hh5', i_d='sui_b7', ser_ver=server, early='earliest'))
        data.append(t_p.submit(get_consume, table='hh5', i_d='sui_b7', ser_ver=server, early='earliest'))
    t_p.shutdown(wait=True)
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save1.csv', 'a')as f:
        for d in data:
            process_id, count, end_time = d.result()
            total_time = end_time - start
            avg_time = total_time / count
            list1 = ['kafka', 'multi process single thread', total_time, avg_time]
            row = list1

            f_csv = csv.writer(f)
            f_csv.writerow(row)

            print('process {} finished in {} seconds total {}message,avg time in {} seconds'.format(process_id,
                                                                                                    total_time, count,
                                                                                                    avg_time))
    print('main id is {}'.format(os.getpid()))


if __name__ == '__main__':
    print('start')
    start_time = time.time()
    server = ['localhost:9092']
    mul_thread()
    end_time = time.time()
    print('main id is {}'.format(os.getpid(), end_time - start_time))
