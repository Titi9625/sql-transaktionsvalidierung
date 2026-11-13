# Bank CSV validation on Azure

A bank CSV uploaded to the private `incoming` container raises a Blob Created event. Event Grid sends it to the Python Azure Function `validate_bank_csv`, which uses the project's existing CSV format rules and writes a JSON report to the private `results` container. The input file stays available for inspection.

The report checks whether fields can be read in the supported format. For example, a decimal amount such as `49.00` passes the amount format check. The reconciliation pipeline handles payment matching and business validation.

## Smoke test

After wiring Event Grid, upload these fixtures into `incoming` with fresh names and check the corresponding files in `results`.

| Input fixture | Expected result |
| --- | --- |
| [Valid bank CSV](../testdaten/batch_scenarios/02_updated/bank.csv) | `passed` with `rows_checked: 4` |
| [Malformed bank CSV](../testdaten/batch_scenarios/03_broken_format/bank.csv) | `rejected` with a reason that identifies data row 4 and column `amount` |

Each input has one output named `<input filename>.json`. A format rejection is a successful check with an actionable report. Use distinct input names when you want to retain separate results.

## Run locally and package

From the repository root in PowerShell, with the project virtual environment available:

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\azure_demo\requirements.txt
.\.venv\Scripts\python.exe -m unittest azure_demo.test_function_app -v
$functionZip = Join-Path $env:TEMP "bank-check-$([guid]::NewGuid().ToString('N')).zip"
.\.venv\Scripts\python.exe .\azure_demo\build_function_zip.py $functionZip
```

The ZIP builder places the Functions entry point, host settings, dependency list and shared checker at their required paths. To run the original manual Blob Storage check, install `azure-identity` and `azure-storage-blob`, sign in with `az login`, and run `python -m azure_demo.check_azure_blob <storage-account> <blob-name>` with permission to read inputs and write results.

## Azure configuration

1. Create a general-purpose storage account with private containers named `incoming` and `results`. Create a Python 3.13 Function App on Flex Consumption in a supported region and enable its system-assigned managed identity. The demo uses Germany West Central. [Flex Consumption setup](https://learn.microsoft.com/en-us/azure/azure-functions/flex-consumption-how-to)
2. Give the app identity `Storage Blob Data Owner` and `Storage Queue Data Contributor` on the demo storage account. Set `BankStorage__blobServiceUri` to its Blob service URL and `BankStorage__queueServiceUri` to its Queue service URL. [Storage connection settings and roles](https://learn.microsoft.com/en-us/azure/azure-functions/manage-connections)
3. Deploy the ZIP with `az functionapp deployment source config-zip --resource-group <group> --name <app> --src $functionZip --build-remote true`. Confirm that `az functionapp function list` shows `validate_bank_csv`. [ZIP deployment](https://learn.microsoft.com/en-us/azure/azure-functions/flex-consumption-how-to)
4. On the storage account, create an Event Grid subscription for `Microsoft.Storage.BlobCreated` with Event Grid Schema. Set its subject prefix to `/blobServices/default/containers/incoming/blobs/` and its suffix to `.csv`. Use the Function App's deployed hostname and the `blobs_extension` system key to build the webhook. Keep the key and full endpoint URL in Azure configuration and out of the repository. [Blob trigger webhook setup](https://learn.microsoft.com/en-us/azure/azure-functions/functions-event-grid-blob-trigger)

## Cost and teardown

The demo uses Flex Consumption with zero always-ready instances. Check usage and student credit in Azure Cost Management. You can set a small budget alert for the demo subscription or resource group. Budget alerts notify you when spending reaches a threshold. [Flex Consumption pricing](https://azure.microsoft.com/en-us/pricing/details/functions/) · [Azure budget alerts](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-acm-create-budgets)

When finished, save any reports you need and open the dedicated demo resource group in the Azure portal. Review its resource list, then use **Delete resource group** to remove the demo app, storage account and test data. [Azure resource group deletion](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/delete-resource-group)
