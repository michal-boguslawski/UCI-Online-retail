import json
import os
from typing import Union

def replace_placeholders(obj: Union[dict, list, str], placeholders: dict) -> Union[dict, list, str]:
    """Recursively replace placeholder values using Config class attributes."""
    if isinstance(obj, dict):
        return {k: replace_placeholders(v, placeholders) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_placeholders(i, placeholders) for i in obj]
    elif isinstance(obj, str):
        return placeholders.get(obj, obj)

placeholders = {
    "s3_bucket_name": os.getenv('S3_BUCKET_NAME'),
    "s3_region": os.getenv("S3_REGION"),
    "kafka_topic": os.getenv("KAFKA_TOPIC"),
    "kafka_topic_dir": os.getenv("KAFKA_TOPIC_DIR"),
}


# Load JSON from file or string
with open("s3-sink_template.json") as f:
    config_data = json.load(f)

# Replace placeholders
final_config = replace_placeholders(config_data, placeholders)

# Write Json
with open("s3-sink.json", "w") as f:
    json.dump(final_config, f, indent=2)
