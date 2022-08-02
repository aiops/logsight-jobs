
# set base image (host OS)
FROM python:3.8

RUN apt-get update && \
    apt-get -y install --no-install-recommends libc-bin git-lfs && \
    rm -r /var/lib/apt/lists/*

# set the working directory in the container
WORKDIR /code

COPY ./requirements.txt .
# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh pip install git+ssh://git@github.com/aiops/logsight.git@lib#egg=logsight

# copy code
COPY ./logsight_jobs logsight_jobs
# copy entrypoint.sh
COPY entrypoint.sh .

# Set logsight home dir
ENV LOGSIGHT_HOME="/code/logsight_jobs"
ENV PYTHONPATH="/code"

ENTRYPOINT [ "./entrypoint.sh" ]
