import time
from locust import HttpUser, TaskSet, task
class UserTasks(TaskSet):
    @task(1)
    def index(self):
        self.client.post("/publishEvent",
                         json = {"eventId":"testEvent",
                          "eventMessage":"load-test",
                          "eventTime": time.time()
                          })

class WebsiteUser(HttpUser):
    tasks = [UserTasks]
    min_wait = 5000
    max_wait = 9000