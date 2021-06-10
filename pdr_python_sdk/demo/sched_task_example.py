import yaml
from pdr_python_sdk.client import *
from pdr_python_sdk.schedule.job import Job
from pdr_python_sdk.schedule.task import Task

"""
Demo connection params

params = {
    "scheme": "http",
    "host": "pandora-express-rc.qiniu.io",
    "port": 80,
    "token": "xxxxx"
}
"""
f = open("config.yaml", 'r')
params = yaml.load(f, Loader=yaml.FullLoader)
f.close()
conn = connect(**params)

job = Job('demoJobName')
job.set_app_name('demoAppName')
job.set_parameter('action', 'run')
job.set_crontab('0 0/2 * * * ?')
job.set_status(True)
job.set_execute_batch(2)

task_1a = Task('customHandler')
task_1a.set_parameter('file', 'send_msg.py')
task_1a.set_parameter('env', 'user=jack')
task_1a.set_parameter('params', {'money': 100, 'type': 'RMB'})

task_2a = Task('customHandler')
task_2a.set_parameter('file', 'putin_agree.py')
task_2a.set_parameter('env', 'user=Putin')
task_2a.set_parameter('params', {'action': 'agree'})
task_2a.add_task(task_1a)

task_3a = Task('customHandler')
task_3a.set_parameter('file', 'trump_agree.py')
task_3a.set_parameter('env', 'user=Trump')
task_3a.set_parameter('params', {'action': 'agree'})
task_3a.add_task(task_1a)

job.add_task(task_2a)
job.add_task(task_3a)

print('create job, return : ' + str(conn.create_schedule(job)))

job.set_parameter('action', 'stop')
print('update job, return : ' + str(conn.update_schedule(job)))

print('get job: return : ' + str(conn.get_schedule(job.get_job_id())))

print('delete job: return : ' + str(conn.delete_schedule(job.get_job_id())))
