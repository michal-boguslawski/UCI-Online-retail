FROM python:3.13.3
WORKDIR /user/local/app

COPY requirements.txt create_topic.py start.sh helper_functions.py produce.py wait-for-services.sh ./

RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x start.sh

ENTRYPOINT ["./wait-for-services.sh"]
CMD [ "./start.sh" ]