FROM python:3

ADD run.py /
ENV BUCKET=""
RUN pip install boto3 progressbar2

CMD [ "python", "./run.py" ]
