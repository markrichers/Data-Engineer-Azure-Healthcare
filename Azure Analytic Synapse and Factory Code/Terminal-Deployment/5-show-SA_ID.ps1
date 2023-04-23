
$storage_acct_name='functionsdemodata'
$resource_group_name='rg-cloudetl-demo'

$storage_acct_id=$(az storage account show --name $storage_acct_name --resource-group $resource_group_name --query 'id' --output tsv)

Write-Output $storage_acct_id