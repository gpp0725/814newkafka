import time

from confluent_kafka import Producer
from .save.save import save_data

def obtain_server():
    c = Producer({
        'bootstrap.servers': my_broker,
        'group.id': my_group,
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    })
    return c


def get_txt():
    with open(file_path, 'rb')as f_:
        yield from f_.readlines()


def con_pro2():
    count = 0
    star_tm = time.time()
    c = obtain_server()
    for index, line in enumerate(get_txt()):
        if line:
            count += 1
            # print(index, line, count)
            c.produce(table, line)
    c.flush()
    end_tm = time.time()
    total_tm = end_tm - star_tm
    return count, total_tm, total_tm / count


if __name__ == '__main__':
    my_broker = "127.0.0.1:9092"
    my_group = 'my_group'
    table = 'mgj1'
    file_path = r'/tmp/tmp/pycharm_project_4/814new/enhance/Harry Potter.txt'
    save_data_path = '/tmp/tmp/pycharm_project_4/814new/enhance/save/save9.csv'
    co_unt, to_tal, avg_tm = con_pro2()  # obtain produce data,time

    list1 = ['confluent_kafka ', 'Single process single thread', to_tal, avg_tm]
    row = list1
    save_data(save_data_path, row)
    print('A total of {} messages were produced in {} seconds,'
          ' with an average of {} second per message.'.format(co_unt,
                                                              to_tal,
                                                              avg_tm))
