import csv
from pykafka import KafkaClient
import time


def consume():
    start_time = time.time()
    co_unt = 0
    with open('py_kafkamu.csv', 'a')as f:
        for message in consumer:
            co_unt += 1
            row = [message.partition, message.offset, message.value]
            f_cv = csv.writer(f)
            f_cv.writerow(row)
            if co_unt >= 81227:
                break

            # print(message.partition, message.offset, message.value)
    total_time = time.time() - start_time
    return co_unt, total_time, total_time / co_unt


if __name__ == '__main__':
    client = KafkaClient(hosts="127.0.0.1:9092", socket_timeout_ms=30 * 1000, )
    topic = client.topics[b'test4']
    consumer = topic.get_simple_consumer()
    count, total, avg = consume()
    list1 = ['pykafka', 'Single consumer single thread', total, avg]
    header = ['tools', 'With', 'total_consume_time(s)', 'avg_consume_time(s)']
    row = list1
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save8.csv', 'w')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(header)
        f_csv.writerow(row)
    print('A total of {} messages were consumed in {} seconds, with an average of {} second per message.'.format(count,
                                                                                                                 total,
                                                                                                                 avg))
