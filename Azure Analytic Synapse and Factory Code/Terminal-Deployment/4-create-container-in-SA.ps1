$abs_container_name='demo-functionetl-data'
$abs_archive_container_name='demo-functionetl-archive'
$storage_acct_name='functionsdemodata'

# Create a storage container in a storage account.
az storage container create --name $abs_container_name --account-name $storage_acct_name --auth-mode login

az storage container create --name $abs_archive_container_name --account-name $storage_acct_name --auth-mode login

