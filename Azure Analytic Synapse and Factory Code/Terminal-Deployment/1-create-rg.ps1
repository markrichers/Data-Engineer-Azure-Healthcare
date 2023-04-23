$service_location='westeurope'
$resource_group_name='rg-cloudetl-demo'

# Create an Azure Resource Group to organize the Azure services used in this series logically
az group create --location $service_location --name $resource_group_name