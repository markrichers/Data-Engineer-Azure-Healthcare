$storage_acct_name='functionsdemodata'
$resource_group_name='rg-cloudetl-demo'

# Capture storage account access key1
az storage account keys list --resource-group $resource_group_name --account-name $storage_acct_name --query [0].value --output tsv