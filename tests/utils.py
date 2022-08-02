import random
from datetime import datetime


def random_times(start, end, n):
    frmt = '%Y-%m-%d %H:%M:%S'
    stime = datetime.strptime(start, frmt)
    etime = datetime.strptime(end, frmt)
    td = etime - stime
    return [random.random() * td + stime for _ in range(n)]
