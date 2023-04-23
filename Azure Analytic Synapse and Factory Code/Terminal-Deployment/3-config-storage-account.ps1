$user_email='432469_student.fontys.nl#EXT#@822z0l.onmicrosoft.com'
$resource_group_name='rg-cloudetl-demo'

# Assign the 'Storage Blob Data Contributor' role to your user
az role assignment create --assignee $user_email --role 'Storage Blob Data Contributor' --resource-group  $resource_group_name