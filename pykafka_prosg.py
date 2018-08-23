import threading

import csv
from pykafka import KafkaClient
import time


def get_txt():
    with open(r'/tmp/tmp/pycharm_project_4/814new/enhance/Harry Potter.txt', 'rb')as f:
        yield from f.readlines()


def produce():
    client = KafkaClient(hosts="127.0.0.1:9092")
    topic = client.topics[b'hh3']
    producer = topic.get_producer()
    count = 0
    star_time = time.time()
    for line in get_txt():
        if len(line) != 0:
            count += 1
            # print(line)
        producer.produce(line)
    end_time = time.time()
    return count, end_time - star_time, (end_time - star_time) / count


if __name__ == '__main__':
    count, total_time, avg = produce()
    list1 = ['pykafka', 'Single process single thread', total_time, avg]
    header = ['tools', 'With', 'total_produce_time(s)', 'avg_produce_time(s)']
    row = list1
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save9.csv', 'w')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(header)
        f_csv.writerow(row)

    print('total {} message thread {} ended {} and avg_msg_time {}'.format(count, threading.current_thread().name,
                                                                           total_time,
                                                                           avg))
