import json
from typing import Any, Dict, List

import redis


def get_redis_client() -> redis.Redis:
    return redis.StrictRedis(
        host="localhost",
        port=6379,
    )


def get_data(redis_client: redis.Redis, dataset_name: str, version: str) -> List[Dict[str, Any]]:
    pattern = f"serving_data:{dataset_name}:{version}:*"
    cursor = 0
    data = []

    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)

        for key in keys:
            value = json.loads(redis_client.get(key))
            data.append(value)

        if cursor == 0:
            break

    return data


def get_current_version(redis_client: redis.Redis, dataset_name: str) -> str:
    version = redis_client.get(f"current_version:{dataset_name}")
    return version.decode("utf-8")


if __name__ == "__main__":
    redis_client = get_redis_client()

    dataset_name = "iris"
    version = get_current_version(redis_client=redis_client, dataset_name=dataset_name)

    print(get_data(redis_client=redis_client, dataset_name=dataset_name, version=version))
