from multiprocessing import Pool, TimeoutError, freeze_support
import time
import os
import subprocess

source_host = os.environ['SOURCE_HOST']
source_user = os.environ['SOURCE_USER']
source_pass = os.environ['SOURCE_PASS']
folder = os.environ['FOLDER']
segments_per_thread = int(os.environ['SEG_PER_THREAD'])
no_of_subprocesses = int(os.environ['NO_OF_PROCESSES'])

def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [ alist[ i * length // wanted_parts: (i + 1) * length // wanted_parts ] 
             for i in range(wanted_parts) ]

def process_table_list(index, local_tables) : 
    print(index)
    print(len(local_tables))
    split_local_tables = split_list(local_tables, segments_per_thread)
    for x in split_local_tables :
        dump_table_list = ""
        for table_x in x:
            table_str = table_x.decode("utf-8") 
            table_str = table_str.strip()
            dump_table_list = dump_table_list + table_str + " "
        dump_command = "mkdir -p {folder};export MYSQL_PWD={source_pass};mysqldump -h {source_host} -u {source_user} --no-tablespaces --single-transaction --skip-triggers --skip-lock-tables colbi_repo {dump_table_list}>> {folder}/colbi_repo_{index}.sql".format(source_host = source_host, source_user = source_user, source_pass = source_pass, folder = folder, index = index, dump_table_list = dump_table_list)
        os.system(dump_command)

start = time.time()
table_command = "export MYSQL_PWD={source_pass}; mysql -h {source_host} -u {source_user} -s -e \"select table_name from information_schema.tables where table_schema = 'colbi_repo'\"".format(source_host = source_host, source_user = source_user, source_pass = source_pass)

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
print ("Time to backup table details")
print(end - start)
