import csv
import time

from confluent_kafka import Producer

my_broker = "127.0.0.1:9092"

c = Producer({
    'bootstrap.servers': my_broker,
    'group.id': 'my_group1',
    # 'client.id': 'lanyang',
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
})


def get_txt():
    with open(r'/tmp/tmp/pycharm_project_4/814new/enhance/Harry Potter.txt', 'rb')as f:
        yield from f.readlines()


def con_pro2():
    count = 0
    star_tm = time.time()
    for index, line in enumerate(get_txt()):
        # line = line.replace('\n', '').replace('\t', '').replace('\r', '').strip()
        if len(line) != 0:
            count += 1
            # print(index, line, count)
            c.produce('hh2', line)
    c.flush()
    end_tm = time.time()
    total_tm = end_tm - star_tm
    return count, total_tm, total_tm / count


if __name__ == '__main__':
    co_unt,to_tal,avg_tm = con_pro2()
    list1 = ['confluent_kafka ', 'Single process single thread', to_tal, avg_tm]
    row = list1
    with open('/tmp/tmp/pycharm_project_4/814new/enhance/save/save9.csv', 'a')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(row)
    print('A total of {} messages were produced in {} seconds,'
          ' with an average of {} second per message.'.format(co_unt,
                                                              to_tal,
                                                              avg_tm))


# zhengchang