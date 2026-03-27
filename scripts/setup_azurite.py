#!/usr/bin/env python
"""Create Azurite test container and generate a SAS token.

Writes AZURITE_SAS_TOKEN to $GITHUB_ENV for use in subsequent GHA steps.
"""
import os
import time
from datetime import datetime, timedelta, timezone

from azure.storage.blob import (
    BlobServiceClient,
    ContainerSasPermissions,
    generate_container_sas,
)

# Storage account test info from:
# https://learn.microsoft.com/en-us/azure/storage/common/storage-connect-azurite

ACCOUNT_NAME = "devstoreaccount1"
ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
    "/K1SZFPTOtr/KBHBeksoGMGw=="
)
CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    f"AccountName={ACCOUNT_NAME};"
    f"AccountKey={ACCOUNT_KEY};"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"
)
CONTAINER_NAME = "testcontainer"


def create_container():
    for attempt in range(10):
        try:
            client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
            client.create_container(CONTAINER_NAME)
            print("Container created")
            return
        except Exception as e:
            if "ContainerAlreadyExists" in str(e):
                print("Container already exists")
                return
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    raise RuntimeError("Failed to create container")


def generate_sas():
    return generate_container_sas(
        account_name=ACCOUNT_NAME,
        container_name=CONTAINER_NAME,
        account_key=ACCOUNT_KEY,
        permission=ContainerSasPermissions(
            read=True, write=True, delete=True, list=True, create=True
        ),
        expiry=datetime.now(timezone.utc) + timedelta(hours=1),
    )


def main():
    create_container()
    sas = generate_sas()

    github_env = os.environ.get("GITHUB_ENV")
    if github_env:
        with open(github_env, "a") as f:
            f.write(f"AZURITE_SAS_TOKEN={sas}\n")
        print("SAS token written to GITHUB_ENV")
    else:
        print(f"AZURITE_SAS_TOKEN={sas}")


if __name__ == "__main__":
    main()
