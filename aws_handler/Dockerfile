FROM python:3.13-alpine3.22

RUN pip install boto3
COPY packages ./packages
COPY main.py .

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD [ "python", "main.py" ]
