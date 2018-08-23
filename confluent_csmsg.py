import csv

from confluent_kafka import Consumer, KafkaError

import time


def conf_csm():
    star_time = time.time()
    c = Consumer({
        'bootstrap.servers': my_broker,
        'group.id': 'my_g',
        # 'client.id': 'lanyang',
        'default.topic.config': {
            'auto.offset.reset': 'beginning',
            # 'consumer_timeout_ms': 2000,
        }
    })
    c.subscribe(['test4'])
    count = 0
    with open('py_kafkamo.csv', 'a')as f:
        while True:
            msg = c.poll(1)
            count += 1
            if msg is None:
                continue
            if count >= 81227:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            row = [msg.partition, msg.offset(), msg.value()]
            f_cv = csv.writer(f)
            f_cv.writerow(row)
            # print(msg.offset(), msg.value())
    c.close()
    end_tm = time.time()
    to_tal_tm = end_tm - star_time

    return count, to_tal_tm, to_tal_tm / count


if __name__ == '__main__':
    my_broker = "127.0.0.1:9092"
    co_unt, total_tm, avg_tm = conf_csm()
    list1 = ['confluent-kafka', 'Single consumer single thread', total_tm, avg_tm]
    row = list1
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save8.csv', 'a')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(row)
    print('A total of {} messages were consumed in {} seconds,'
          ' with an average of {} second per message.'.format(co_unt,
                                                              total_tm,
                                                              avg_tm))
