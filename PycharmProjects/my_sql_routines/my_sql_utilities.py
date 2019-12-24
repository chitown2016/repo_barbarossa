__author__ = 'kocat_000'
import mysql.connector
from math import ceil
import shared.directory_names_aux as dna
import os as os


def get_my_sql_connection(**kwargs):

    if 'con' in kwargs.keys():
        con = kwargs['con']
    else:
        db_host = '127.0.0.1'
        db_user = 'ekocatulum'
        db_pass = 'caesar1789'
        db_name = 'futures_master'
        con = mysql.connector.connect(user=db_user, password=db_pass, host=db_host, database=db_name,auth_plugin='mysql_native_password')
    return con


def sql_execute_many_wrapper(**kwargs):

    final_str = kwargs['final_str']
    tuples = kwargs['tuples']

    if 'num_item_per_load' in kwargs.keys():
        num_item_per_load = execute_input['num_item_per_load']
    else:
        num_item_per_load = 100

    con = get_my_sql_connection(**kwargs)
    cur = con.cursor()

    for i in range(0, int(ceil(len(tuples) / num_item_per_load))):
        cur.executemany(final_str, tuples[i*num_item_per_load:(i+1)*num_item_per_load])
    con.commit()

    if 'con' not in kwargs.keys():
        con.close()


def dropbox_backup(**kwargs):

    box_no = kwargs['box_no']

    backup_path = dna.get_directory_name(ext='drop_box_trading') + "/mysql/backups"
    backup_path = backup_path.replace("\\","/")

    db_host = 'localhost'
    db_user = 'ekocatulum'
    db_pass = 'caesar1789'
    db_name = 'futures_master'

    dumpcmd = "mysqldump -h " + db_host + " -u " + db_user + " -p" + db_pass + " " + db_name + " trades > " + backup_path + "/trades" + str(box_no) + ".sql"
    os.system(dumpcmd)

    dumpcmd = "mysqldump -h " + db_host + " -u " + db_user + " -p" + db_pass + " " + db_name + " strategy > " + backup_path + "/strategy" + str(box_no) + ".sql"
    os.system(dumpcmd)
