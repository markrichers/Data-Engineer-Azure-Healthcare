$storage_acct_name='functionsdemodata'
$abs_container_name='demo-functionetl-data'

az storage blob upload --account-name $storage_acct_name --container-name $abs_container_name --name 'financial_sample.csv' --file 'C:\Users\achen\Documents\1_Fontys_Docs\S7_DDBL_data_science_and_visualization_docs\Health_Wearbles_Docs\Azure_Infrastructure_DDBL\FunctionTest1\powershell-deploy\financial_sample.csv' --auth-mode login