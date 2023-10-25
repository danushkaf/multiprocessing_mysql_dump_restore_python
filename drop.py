from multiprocessing import Pool, TimeoutError, freeze_support
import time
import os
import subprocess

target_host = os.environ['TARGET_HOST']
target_user = os.environ['TARGET_USER']
target_pass = os.environ['TARGET_PASS']
folder = os.environ['FOLDER']
no_of_subprocesses = int(os.environ['NO_OF_PROCESSES'])

def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [ alist[ i * length // wanted_parts: (i + 1) * length // wanted_parts ] 
             for i in range(wanted_parts) ]

def process_table_list(index, local_tables) : 
    for table_x in local_tables :
        table_str = table_x.decode("utf-8") 
        table_str = table_str.strip()
        drop_file_command = "echo 'DROP TABLE colbi_repo.{table_name};' >> {folder}/drops_{index}.sql;".format(folder = folder, index = index, table_name = table_str)
        os.system(drop_file_command)
    time.sleep(5)
    drop_command = "export MYSQL_PWD={target_pass};mysql -h {target_host} -u {target_user} colbi_repo < {folder}/drops_{index}.sql".format(target_host = target_host, target_user = target_user, target_pass = target_pass, folder = folder, index = index)
    os.system(drop_command)

start = time.time()
table_command = "export MYSQL_PWD={target_pass}; mysql -h {target_host} -u {target_user} -s -e \"select table_name from information_schema.tables where table_schema = 'mydatabase'\"".format(target_host = target_host, target_user = target_user, target_pass = target_pass)


os.system("rm -rf {folder};mkdir -p {folder}".format(folder = folder))

sp = subprocess.Popen(['/bin/bash', '-c', table_command], stdout=subprocess.PIPE)
tables = sp.stdout.readlines()
print(len(tables))
split_tables = split_list(tables, wanted_parts=no_of_subprocesses)
poolmap = []
for i in range(no_of_subprocesses):
    poolmap.append((i, split_tables[i]))

pool = Pool(processes=no_of_subprocesses)
pool.starmap(process_table_list, poolmap)
pool.close()
pool.join()
end = time.time()
print ("Time to delete table details")
print(end - start)
