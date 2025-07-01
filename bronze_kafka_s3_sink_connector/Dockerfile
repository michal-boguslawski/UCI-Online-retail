FROM confluentinc/cp-kafka-connect:7.9.2
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.6.6

WORKDIR /home/appuser
COPY start.sh config/s3-sink_template.json packages/config_filler.py ./

CMD ["sh", "start.sh"]
