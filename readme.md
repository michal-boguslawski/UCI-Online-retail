# UCI Retail Data Engineering and Machine Learning
Create a data pipeline that feeds information into a data warehouse. Using machine learning techniques on this data, the pipeline generates predictive insights to support decision-making.

## Project technology stack
- Python
- Kafka
- PySpark
- AWS S3
- AWS BOTO3
- Docker
- Jupyter Notebooks

## Project Diagram

```
retail-demand-forecasting/
├── docker-compose.yaml 
├── .env ✅ 
├── config.py ✅ 
├───aws_handler ✅ 
│   └───packages ✅ 
├───bronze_kafka_s3_sink_connector ✅ 
│   ├───config ✅ 
│   ├───packages ✅ 
├───data_generator ✅ 
│   ├───packages ✅ 
├───EDA ✅ 
├───secrets ✅ 
├───silver_data_transforms ✅ 
│   └───packages ✅ 
├───gold_data_transforms 🔴
│   └───packages 🔴
├───spark_streaming_job 🔴
├── ml_train/ 🔴
├── model_serving/ 🔴 # FastApi
└── infra/ 🔴
```
```
┌──────────────┐      ┌────────────────┐      ┌────────────────┐
│ Data Gen     │─►──► │ Kafka (+ZK)    │─►──► │ Kafka Connect  │
│ (Python)     │      │   container    │      │   container    │
└──────────────┘      └────────────────┘      └─────┬──────────┘
                                                    │ writes
┌─────────────────────┐           ┌─────────────────▼───────────┐
│ NoSQL Store         │◄───────── ┤            S3               │
│ (DynamoDB-local or  │   cache   │                             │
│  MongoDB) container │           └──────┬──────────────────────┘
└─────────────▲───────┘                  │ off-line
              │ realtime                 │ batch
              ▼                          ▼
        ┌────────────┐           ┌────────────────┐
        │ Model API  │◄──────────┤ ML training    │
        │ (FastAPI)  │   model   │  (Python)      │
        └────────────┘           └────────────────┘
                   ▲
                   │ REST / gRPC
                   ▼
            ┌────────────┐
            │ Dashboard  │  (optional)
            └────────────┘
```
Stretch Goals / Extensions
- Integrate SNS/SQS for alerts on stock shortages.
- Add geographical clustering of products using k-means.
- Add A/B testing for ML models using endpoints.
