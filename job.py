class Job():
    def __init__(self, glueContext):
        print("Job init")
    def init(self, job_name, args):
        print("invoking job init...")
    def commit(self):
        print("job commit")