$funcapp_name='func-demoazure'
$key_vault_name='kv-functionetl-demo'
$resource_group_name='rg-cloudetl-demo'

# Generate managed service identity for function app
az functionapp identity assign --resource-group $resource_group_name --name $funcapp_name

# Capture function app managed identity id
$func_principal_id = az resource list --name $funcapp_name --query [*].identity.principalId --output tsv
Write-Output $func_principal_id

# Capture key vault object/resource id
$kv_scope = az resource list --name $key_vault_name --query [*].id --output tsv

# set permissions policy for function app to key vault - get list and set
az keyvault set-policy --name $key_vault_name --resource-group $resource_group_name --object-id $func_principal_id --secret-permission get list set

## Set IAM role 

# Create a 'Key Vault Contributor' role assignment for function app managed identity
az role assignment create --assignee $func_principal_id --role 'Key Vault Contributor' --scope $kv_scope

# Assign the 'Storage Blob Data Contributor' role to the function app managed identity
az role assignment create --assignee $func_principal_id --role 'Storage Blob Data Contributor' --resource-group  $resource_group_name

# Assign the 'Storage Queue Data Contributor' role to the function app managed identity
az role assignment create --assignee $func_principal_id --role 'Storage Queue Data Contributor' --resource-group  $resource_group_name

