import json
import uuid


class Job:
    def __init__(self, name):
        self.jobId = str(uuid.uuid4())
        self.jobName = name
        self.appName = None
        self.globalParameters = {}
        self.enabled = True
        self.executeBatch = 0
        self.crontab = '*/5 * * * * ?'
        self.tasks = []

    def set_job_id(self, job_id):
        self.jobId = job_id

    def get_job_id(self):
        return self.jobId

    def set_app_name(self, app_name):
        self.appName = app_name

    def get_app_name(self):
        return self.appName

    def set_parameter(self, key, value):
        self.globalParameters[key] = value

    def get_parameter(self, key):
        return self.globalParameters[key]

    def get_parameters(self):
        return self.globalParameters

    def set_status(self, status):
        self.enabled = status

    def get_status(self):
        return self.enabled

    def set_execute_batch(self, batches):
        self.executeBatch = batches

    def get_execute_batch(self):
        return self.executeBatch

    def set_crontab(self, crontab):
        self.crontab = crontab

    def get_crontab(self):
        return self.crontab

    def add_task(self, task):
        self.tasks.append(task)

    def job2dict(self):
        job_dict = {
            'jobId': self.jobId,
            'jobName': self.jobName,
            'appName': self.appName,
            'globalParameters': self.globalParameters,
            'enabled': self.enabled,
            'executeBatch': self.executeBatch,
            'crontab': self.crontab,
            'tasks': []
        }

        if len(self.tasks) > 0:
            tasks = []
            for task in self.tasks:
                tasks.append(task.task2dict())
            job_dict['tasks'] = tasks

        return job_dict
