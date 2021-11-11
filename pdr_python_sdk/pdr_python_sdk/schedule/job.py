import uuid

from pdr_python_sdk.schedule.task import Task

class Job:
    def __init__(self, name):
        self.job_id = str(uuid.uuid4())
        self.job_name = name
        self.app_name = None
        self.global_parameters = {}
        self.enabled = True
        self.execute_batch = 0
        self.crontab = '*/5 * * * * ?'
        self.tasks = []

    def set_job_id(self, job_id):
        self.job_id = job_id

    def get_job_id(self):
        return self.job_id

    def set_app_name(self, app_name):
        self.app_name = app_name

    def get_app_name(self):
        return self.app_name

    def set_parameter(self, key, value):
        self.global_parameters[key] = value

    def get_parameter(self, key):
        return self.global_parameters[key]

    def get_parameters(self):
        return self.global_parameters

    def set_status(self, status):
        self.enabled = status

    def get_status(self):
        return self.enabled

    def set_execute_batch(self, batches):
        self.execute_batch = batches

    def get_execute_batch(self):
        return self.execute_batch

    def set_crontab(self, crontab):
        self.crontab = crontab

    def get_crontab(self):
        return self.crontab

    def add_task(self, task):
        self.tasks.append(task)

    def job2dict(self):
        job_dict = {
            'jobId': self.job_id,
            'jobName': self.job_name,
            'appName': self.app_name,
            'globalParameters': self.global_parameters,
            'enabled': self.enabled,
            'executeBatch': self.execute_batch,
            'crontab': self.crontab,
            'tasks': None
        }

        if self.tasks:
            tasks = []
            for task in self.tasks:
                tasks.append(task.task2dict())
            job_dict['tasks'] = tasks

        return job_dict

    def json2job(self, json_data):
        self.job_id = json_data['jobId']
        self.job_name = json_data['jobName']
        self.app_name = json_data['appName']
        self.global_parameters = json_data['globalParameters']
        self.enabled = json_data['enabled']
        self.execute_batch = json_data['executeBatch']
        self.crontab = json_data['crontab']
        if json_data['tasks']:
            for task_json_data in json_data['tasks']:
                self.tasks.append(Task(None).json2task(task_json_data))
        return self
