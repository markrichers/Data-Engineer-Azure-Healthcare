$adls_acct_name='dlsfunctionetldemo'
$resource_group_name='rg-cloudetl-demo'

az storage account keys list --resource-group $resource_group_name --account-name $adls_acct_name --query [0].value --output tsv
