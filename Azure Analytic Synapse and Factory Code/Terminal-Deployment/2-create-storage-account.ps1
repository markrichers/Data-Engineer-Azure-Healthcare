$storage_acct_name='functionsdemodata'
$resource_group_name='rg-cloudetl-demo'
$service_location='westeurope'


# Create a general-purpose storage account in your resource group and assign it an identity
az storage account create --name $storage_acct_name --resource-group $resource_group_name --location $service_location --sku Standard_LRS --assign-identity