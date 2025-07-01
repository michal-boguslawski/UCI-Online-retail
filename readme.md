retail-demand-forecasting/ \n
├── docker-compose.yaml \n
├── .env.example \n
├── data_generator/ \n
│   ├── Dockerfile \n
│   └── produce.py \n
├── spark_streaming_job/ \n
│   ├── Dockerfile          # optional – packaged Spark job \n
│   └── streaming_job.py \n
├── ml_train/ \n
│   ├── Dockerfile \n
│   └── train.py \n
├── model_serving/ \n
│   ├── Dockerfile \n
│   ├── main.py             # FastAPI app \n
│   └── requirements.txt \n
└── infra/ \n
    └── cdk/terraform for prod \n

┌──────────────┐      ┌────────────────┐      ┌────────────────┐ \n
│ Data Gen     │─►──► │ Kafka (+ZK)    │─►──►│ Spark Streaming │ \n
│ (Python)     │      │   container    │      │   container    │ \n
└──────────────┘      └────────────────┘      └─────┬──────────┘ \n
                                                     │ writes \n
┌─────────────────────┐          ┌───────────────────▼──────────┐ \n
│ NoSQL Store         │◄─────────┤ S3-compatible blob storage   │ \n
│ (DynamoDB-local or  │   cache   │ (MinIO locally, S3 in prod) │ \n
│  MongoDB) container │           └──────┬──────────────────────┘ \n
└─────────────▲───────┘                  │ off-line \n
              │ realtime                 │ batch \n
              ▼                          ▼ \n
        ┌────────────┐           ┌────────────────┐ \n
        │ Model API  │◄──────────┤ ML training    │ \n
        │ (FastAPI)  │   model   │  (Python)      │ \n
        └────────────┘           └────────────────┘ \n
                   ▲ \n
                   │ REST / gRPC \n
                   ▼ \n
            ┌────────────┐ \n
            │ Dashboard  │  (optional) \n
            └────────────┘ \n

Stretch Goals / Extensions
Integrate SNS/SQS for alerts on stock shortages.

Add geographical clustering of products using k-means.

Add A/B testing for ML models using endpoints.