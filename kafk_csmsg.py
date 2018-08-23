import csv
import time

from kafka import KafkaConsumer


def con_sm():
    consumer = KafkaConsumer('test3',
                             group_id='consumer-999',
                             bootstrap_servers=['localhost:9092'],
                             consumer_timeout_ms=2000,
                             auto_offset_reset='earliest', enable_auto_commit=False, )
    count = 0
    nu_ll = 0
    star_tm = time.time()
    with open('kafka_csmmore.csv', 'a')as f:
        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            # if message is None:
            #     nu_ll += 1
            # if count > 81227:
            #     break
            count += 1
            ro_w = [message.partition, message.offset, message.value]
            f_cv = csv.writer(f)
            f_cv.writerow(ro_w)
        # print(message.topic, message.partition, message.offset, message.key, message.value)
    end_time = time.time()
    total_tm = end_time - star_tm
    return count, total_tm, total_tm / count


if __name__ == '__main__':
    co_unt, to_tal, avg_tm = con_sm()
    list1 = ['kafka', 'Single consumer single thread', to_tal, avg_tm]
    row = list1
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save8.csv', 'a')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(row)
    print('A total of {} messages were consumed in {} seconds, with an average of {} second per message.'.format(co_unt,
                                                                                                                 to_tal,
                                                                                                                 avg_tm))

# list1 = [' kafka-python', 'Single process single thread', '', '', total_time, avg_time]
# row = list1
# with open('/tmp/tmp/pycharm_project_4/814new/sava/save6.csv', 'w')as f:
#     f_csv = csv.writer(f)
#     f_csv.writerow(row)


# zhengchang