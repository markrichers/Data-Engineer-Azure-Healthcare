$adls_acct_name='dlsfunctionetldemo'

$fsys_name='processed-data-demo'
$dir_name='finance_data'

# Create a file system in ADLS Gen2
az storage fs create --name $fsys_name --account-name $adls_acct_name --auth-mode login

# Create a directory in ADLS Gen2 file system
az storage fs directory create --name $dir_name --file-system $fsys_name --account-name $adls_acct_name --auth-mode login