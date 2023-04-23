$funcapp_name='func-demoazure'

$resource_group_name='rg-cloudetl-demo'
$service_location='westeurope'
# az functionapp delete --name $funcapp_name --resource-group $resource_group_name 
# Create a serverless function app in the resource group.
az functionapp create --name $funcapp_name --storage-account $storage_acct_name --consumption-plan-location $service_location --resource-group $resource_group_name --os-type Linux --runtime python --runtime-version 3.9 --functions-version 4