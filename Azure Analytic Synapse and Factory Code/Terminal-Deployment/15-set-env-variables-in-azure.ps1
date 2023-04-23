
# Update function app's settings to include Azure Key Vault environment variable.
az functionapp config appsettings set --name func-demoazure --resource-group rg-cloudetl-demo --settings "KEY_VAULT_NAME=kv-cloudetl-demo"

# Update function app's settings to include Azure Blob Storage Access Key in Azure Key Vault secret environment variable.
az functionapp config appsettings set --name func-demoazure --resource-group rg-cloudetl-demo --settings  "ABS_SECRET_NAME=abs-access-key1"
