import os
from concurrent.futures import ProcessPoolExecutor

import csv
from pykafka import KafkaClient
import time


def consume(my_broke):
    client = KafkaClient(hosts=my_broke)
    topic = client.topics[b'hh5']  # b'topic'
    consumer = topic.get_simple_consumer()
    count = 0
    with open('py_kafkamore.csv', 'a')as f:
        for message in consumer:
            count += 1
            if count >= 121000:
                break
            row = [message.partition, message.offset, message.value]
            f_cv = csv.writer(f)
            f_cv.writerow(row)
            # print(message.partition, message.offset, message.value.decode('utf-8'))
    return os.getpid(), count, time.time()


def mul_thread():
    data = []
    with ProcessPoolExecutor(max_workers=2)as t_p:
        start = time.time()
        data.append(t_p.submit(consume, my_broke=my_broker))
        data.append(t_p.submit(consume, my_broke=my_broker))

    t_p.shutdown(wait=True)
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save1.csv', 'a')as f:
        for d in data:
            process_id, count, end_time = d.result()
            total_time = end_time - start
            avg_time = total_time / count
            header = ['tools', 'With', 'total_produce_time(s)', 'avg_produce_time(s)']
            list1 = ['pykafka', 'multi process single thread', total_time, avg_time]
            row = list1
            f_csv = csv.writer(f)
            f_csv.writerow(header)
            f_csv.writerow(row)
            print('process {} finished in {} seconds total {}message.'.format(process_id, end_time - start, count))


if __name__ == '__main__':
    print('start')
    star_time = time.time()
    my_broker = "127.0.0.1:9092"
    mul_thread()
    end_tm = time.time()
    print('over', end_tm - star_time)
    # 4.517680883407593