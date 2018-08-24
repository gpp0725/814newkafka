import csv

from confluent_kafka import Consumer, KafkaError

import time


def conf_csm():
    star_time = time.time()
    c = Consumer({
        'bootstrap.servers': my_broker,
        'group.id': group_id,
        'default.topic.config': {
            'auto.offset.reset': 'beginning',
        }
    })
    c.subscribe([table])
    count = 0
    with open(save_consume_path, 'a')as f_:  # Save consumption data to CSV file
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
            row_ = [msg.partition, msg.offset(), msg.value()]
            f_cv = csv.writer(f_)
            f_cv.writerow(row_)
            # print(msg.offset(), msg.value())
    c.close()
    end_tm = time.time()
    to_tal_tm = end_tm - star_time

    return count, to_tal_tm, to_tal_tm / count


if __name__ == '__main__':
    my_broker = "127.0.0.1:9092"
    group_id = 'my_g'
    table = 'test4'
    obtain_time_path = '/tmp/tmp/pycharm_project_4/814new/enhance/save/save8.csv'
    save_consume_path = 'py_kafka_consume.csv'
    co_unt, total_tm, avg_tm = conf_csm()
    list1 = ['confluent-kafka', 'Single consumer single thread', total_tm, avg_tm]
    row = list1
    with open(obtain_time_path, 'a')as f:  # Save the running time and data to the CSV file.
        f_csv = csv.writer(f)
        f_csv.writerow(row)
    print('A total of {} messages were consumed in {} seconds,'
          ' with an average of {} second per message.'.format(co_unt,
                                                              total_tm,
                                                              avg_tm))
