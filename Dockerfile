
# set base image (host OS)
FROM python:3.8

ARG GITHUB_TOKEN
ARG LOGSIGHT_LIB_VERSION

RUN apt-get update && \
    apt-get -y install --no-install-recommends libc-bin git-lfs && \
    rm -r /var/lib/apt/lists/*

# set the working directory in the container
WORKDIR /code

COPY ./requirements.txt .
# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN pip install "git+https://$GITHUB_TOKEN@github.com/aiops/logsight.git@$LOGSIGHT_LIB_VERSION"

# copy code
COPY ./logsight_jobs logsight_jobs
# copy entrypoint.sh
COPY entrypoint.sh .

# Set logsight home dir
ENV LOGSIGHT_HOME="/code/logsight_jobs"
ENV PYTHONPATH="/code"

ENTRYPOINT [ "./entrypoint.sh" ]
