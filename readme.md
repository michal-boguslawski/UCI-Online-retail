retail-demand-forecasting/
├── docker-compose.yaml
├── .env.example
├── data_generator/
│   ├── Dockerfile
│   └── produce.py
├── spark_streaming_job/
│   ├── Dockerfile          # optional – packaged Spark job
│   └── streaming_job.py
├── ml_train/
│   ├── Dockerfile
│   └── train.py
├── model_serving/
│   ├── Dockerfile
│   ├── main.py             # FastAPI app
│   └── requirements.txt
└── infra/
    └── cdk/terraform for prod

┌──────────────┐      ┌────────────────┐      ┌────────────────┐
│ Data Gen     │─►──►│ Kafka (+ZK)    │─►──►│ Spark Streaming │
│ (Python)     │      │   container    │      │   container    │
└──────────────┘      └────────────────┘      └─────┬──────────┘
                                                     │ writes
┌─────────────────────┐          ┌───────────────────▼──────────┐
│ NoSQL Store         │◄─────────┤ S3-compatible blob storage   │
│ (DynamoDB-local or  │   cache   │ (MinIO locally, S3 in prod) │
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

Stretch Goals / Extensions
Integrate SNS/SQS for alerts on stock shortages.

Add geographical clustering of products using k-means.

Add A/B testing for ML models using endpoints.