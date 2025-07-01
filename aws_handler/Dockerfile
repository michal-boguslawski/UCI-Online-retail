FROM python:3.13-alpine3.22

RUN pip install boto3
COPY packages ./packages
COPY main.py .

CMD [ "python", "main.py" ]
