FROM python:3.13-alpine3.22

RUN pip install boto3
COPY create_objects.py .
COPY start.sh .

CMD [ "sh", "start.sh" ]
