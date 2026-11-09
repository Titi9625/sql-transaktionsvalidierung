"""Check a bank CSV from Azure Blob Storage and save its JSON result."""

import argparse
import json

from .check_bank_file import check_bank_file


def check_and_save(service, blob_name: str, *, content_settings=None) -> dict:
    source = service.get_blob_client(container="incoming", blob=blob_name)
    result = check_bank_file(blob_name, source.download_blob().readall())

    # The manual demo keeps the latest check for each input blob name.
    target = service.get_blob_client(container="results", blob=f"{blob_name}.json")
    target.upload_blob(
        json.dumps(result, indent=2).encode("utf-8"),
        overwrite=True,
        content_settings=content_settings,
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("storage_account", help="Azure Storage account name")
    parser.add_argument("blob_name", nargs="?", default="bank.csv")
    args = parser.parse_args()

    from azure.identity import AzureCliCredential
    from azure.storage.blob import BlobServiceClient, ContentSettings

    with AzureCliCredential() as credential, BlobServiceClient(
        account_url=f"https://{args.storage_account}.blob.core.windows.net",
        credential=credential,
    ) as service:
        result = check_and_save(
            service,
            args.blob_name,
            content_settings=ContentSettings(content_type="application/json"),
        )

    print(json.dumps(result, indent=2))
    print(f"Saved to results/{args.blob_name}.json")


if __name__ == "__main__":
    main()
