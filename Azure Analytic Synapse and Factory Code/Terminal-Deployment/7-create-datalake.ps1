$adls_acct_name='dlsfunctionetldemo'
$resource_group_name='rg-cloudetl-demo'
$service_location='westeurope'
#$fsys_name='processed-data-demo'
#$dir_name='finance_data'

# Create a ADLS Gen2 account
az storage account create --name $adls_acct_name --resource-group $resource_group_name --kind StorageV2 --hns --location $service_location --assign-identity