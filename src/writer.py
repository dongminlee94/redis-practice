import json
from typing import Any

import pandas as pd
import redis
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC


def get_redis_client() -> redis.Redis:
    return redis.StrictRedis(
        host="localhost",
        port=6379,
    )


def load_date() -> pd.DataFrame:
    X, y = load_iris(return_X_y=True, as_frame=True)
    X_train, X_valid, y_train, _ = train_test_split(X, y, train_size=0.8)

    scaler = StandardScaler()
    classifier = SVC()

    scaled_X_train = scaler.fit_transform(X_train)
    classifier.fit(scaled_X_train, y_train)

    scaled_X_valid = scaler.transform(X_valid)
    valid_pred = classifier.predict(scaled_X_valid)

    return pd.DataFrame(valid_pred, columns=["valid_pred"])


def save_data(redis_client: redis.Redis, dataset_name: str, version: str, df: pd.DataFrame) -> None:
    data = get_data(redis_client=redis_client, dataset_name=dataset_name, version=version)

    if len(data):
        raise Exception(f"Data already exists for dataset '{dataset_name}' with version '{version}'.")

    pipeline = redis_client.pipeline()

    for i in range(len(df)):
        key = f"serving_data:{dataset_name}:{version}:{i}"
        value = df.iloc[i].to_dict()

        pipeline.set(key, json.dumps(value))

    pipeline.execute()


def delete_data(redis_client: redis.Redis, dataset_name: str, version: str) -> None:
    pattern = f"serving_data:{dataset_name}:{version}:*"
    cursor = 0

    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)

        if keys:
            redis_client.delete(*keys)

        if cursor == 0:
            break


def get_data(redis_client: redis.Redis, dataset_name: str, version: str) -> list[dict[str, Any]]:
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


def set_master_key(redis_client: redis.Redis, dataset_name: str, version: str) -> None:
    redis_client.set(f"current_version:{dataset_name}", version)


def get_master_key(redis_client: redis.Redis, dataset_name: str) -> str:
    version = redis_client.get(f"current_version:{dataset_name}")
    return version.decode("utf-8")


if __name__ == "__main__":
    redis_client = get_redis_client()

    dataset_name = "iris"

    if 1:
        version = "version_1"

        # serving_data = load_date()
        # save_data(redis_client=redis_client, dataset_name=dataset_name, version=version, df=serving_data)
        data = get_data(redis_client=redis_client, dataset_name=dataset_name, version=version)
        if data:
            print(f"Shape: ({len(data)}, {len(data[0])})")

        # set_master_key(redis_client=redis_client, dataset_name=dataset_name, version=version)
        current_version = get_master_key(redis_client=redis_client, dataset_name=dataset_name)
        print(f"Current version: {current_version}")

    if 0:
        version = "version_2"

        serving_data = load_date()
        save_data(redis_client=redis_client, dataset_name=dataset_name, version=version, df=serving_data)
        data = get_data(redis_client=redis_client, dataset_name=dataset_name, version=version)
        print(f"Shape: ({len(data)}, {len(data[0])})")

        old_version = get_master_key(redis_client=redis_client, dataset_name=dataset_name)
        set_master_key(redis_client=redis_client, dataset_name=dataset_name, version=version)
        new_version = get_master_key(redis_client=redis_client, dataset_name=dataset_name)
        print(f"Old version: {old_version} | New version: {new_version}")

        delete_data(redis_client=redis_client, dataset_name=dataset_name, version=old_version)
