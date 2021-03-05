class Job():
    def __init__(self, glueContext):
        print("Job constructor is getting skipped..")
    def init(self, job_name, args):
        print("Job.init is getting skipped..")
    def commit(self):
        print("job commit")