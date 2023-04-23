$key_vault_name='kv-functionetl-demo'
$resource_group_name='rg-cloudetl-demo'

# Provision new Azure Key Vault in our resource group
az keyvault create --name $key_vault_name --resource-group $resource_group_name

$abs_secret_name='abs-access-key1'
$storage_acct_key1="QjlJ4Ff35S4xqmA1YF5NasB74DR8bb2WwJr971yGYNBkqg1IVIIvU5AHTzFmXqiAtOyspbOWkvZz+ASt6TyOoA=="
$adls_secret_name='adls-access-key1'
$adls_acct_key1 = "kec9J1nK8z1L475w27Z2eoPHmb5yu0fQvV6hMFXFE8xuGIUu5GTeUEjuFNPPeb+IePmNhpVllo8w+AStQtGhZA=="

# Create Secret for Azure Blob Storage Account
az keyvault secret set --vault-name $key_vault_name --name $abs_secret_name --value $storage_acct_key1

# Create Secret for Azure Data Lake Storage Account
az keyvault secret set --vault-name $key_vault_name --name $adls_secret_name --value $adls_acct_key1


# export KEY_VAULT_NAME=$key_vault_name
# export ABS_SECRET_NAME=$abs_secret_name
# export ADLS_SECRET_NAME=$adls_secret_name


