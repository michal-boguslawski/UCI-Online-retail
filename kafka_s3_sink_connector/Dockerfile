FROM confluentinc/cp-kafka-connect:7.9.2
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.6.6

WORKDIR /home/appuser
COPY start-kafka-connect.sh /home/appuser/start.sh
COPY s3-sink_template.json .
COPY config_filler.py .

# RUN chmod +x /home/appuser/start.sh
CMD ["sh", "/home/appuser/start.sh"]
