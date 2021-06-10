import uuid


class Task:
    def __init__(self, handler):
        self.id = str(uuid.uuid4())
        self.handler = handler
        self.task_parameters = {}
        self.successors = []

    def set_task_id(self, task_id):
        self.id = task_id

    def get_task_id(self):
        return self.id

    def set_handler(self, handler):
        self.handler = handler

    def get_handler(self):
        return self.handler

    def set_parameter(self, key, value):
        self.task_parameters[key] = value

    def get_parameter(self, key):
        return self.task_parameters[key]

    def get_parameters(self):
        return self.task_parameters

    def add_task(self, task):
        self.successors.append(task)

    def get_tasks(self):
        return self.successors

    def task2dict(self):
        task_dict = {
            'id': self.id,
            'handler': self.handler,
            'taskParameters': self.task_parameters,
            'successors': None
        }

        if len(self.successors) > 0:
            tasks = []
            for task in self.successors:
                tasks.append(task.task2dict())
            task_dict['successors'] = tasks

        return task_dict
