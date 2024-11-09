from typing import Any, Dict

import redis


def get_redis_client() -> redis.Redis:
    return redis.StrictRedis(
        host="localhost",
        port=6379,
    )


def get_db_size(redis_client: redis.Redis) -> int:
    return redis_client.dbsize()


def get_dataset_info(redis_client: redis.Redis) -> Dict[str, Any]:
    pattern = "serving_data:*"
    cursor = 0
    info = {}

    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)

        for key in keys:
            key = key.decode("utf-8")
            _, dataset_name, version, _ = key.split(":")

            if dataset_name not in info:
                info[dataset_name] = {"total": 0, "version": 0}

            info[dataset_name]["total"] += 1

            if not info[dataset_name]["version"]:
                info[dataset_name]["version"] = int(version)

        if cursor == 0:
            break

    return info


def delete_all_keys(redis_client: redis.Redis) -> None:
    redis_client.flushdb()


if __name__ == "__main__":
    redis_client = get_redis_client()

    db_size = get_db_size(redis_client)
    print(f"DB size: {db_size}\n")

    dataset_info = get_dataset_info(redis_client=redis_client)
    for dataset_name, data in dataset_info.items():
        print(f"{dataset_name} dataset")
        print(f"Number of dataset: {data['total']}")
        print(f"Current version: {data['version']}\n")

    # delete_all_keys(redis_client=redis_client)
