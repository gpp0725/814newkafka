import csv


def save_data(save_data_path, ro_w):
    with open(save_data_path, 'a')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(ro_w)
