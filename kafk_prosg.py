import time

import csv
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')


def get_txt():
    with open(r'/tmp/tmp/pycharm_project_4/814new/enhance/Harry Potter.txt', 'rb')as f:
        yield from f.readlines()


def send_data_2_kafka():
    co__unt = 0
    star_time = time.time()
    for index, line in enumerate(get_txt()):
        # line = line.replace('\n', '').replace('\t', '').replace('\r', '').strip()
        if len(line) != 0:
            co__unt += 1
            # print(index, line, co__unt)
            producer.send('sv2', line)
    producer.flush()
    end_time = time.time()
    total_time = end_time - star_time
    return co__unt, total_time, total_time / co__unt


if __name__ == '__main__':
    co__unt, total_tm, avg_tm = send_data_2_kafka()
    list1 = ['kafka', 'Single process single thread', total_tm, avg_tm]
    row = list1
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save9.csv', 'a')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(row)
    print('A total of {} messages were consumed in {} seconds,'
          ' with an average of {} second per message.'.format(co__unt,
                                                              total_tm,
                                                              avg_tm))
    producer.close()

# zhengchang
