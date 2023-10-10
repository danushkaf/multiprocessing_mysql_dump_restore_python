from multiprocessing import Pool, TimeoutError, freeze_support
import time
import os
import subprocess
import time

target_host = os.environ['TARGET_HOST']
target_user = os.environ['TARGET_USER']
target_pass = os.environ['TARGET_PASS']
folder = os.environ['FOLDER']
no_of_subprocesses = int(os.environ['NO_OF_PROCESSES'])

def process_table_list(index) : 
    restore_command = "export MYSQL_PWD={target_pass};mysql -h {target_host} -u {target_user} colbi_repo < {folder}/colbi_repo_{index}.sql".format(target_host = target_host, target_user = target_user, target_pass = target_pass, folder = folder, index = index)
    os.system(restore_command)

start = time.time()
pool = Pool(processes=no_of_subprocesses)
poolmap = []
for i in range(no_of_subprocesses):
    poolmap.append(i)
pool.map(process_table_list, poolmap)
pool.close()
pool.join()
end = time.time()
print ("Time to restore table details")
print(end - start)
