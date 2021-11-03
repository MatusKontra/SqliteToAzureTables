# SqliteToAzureTables

Pushes contents of a given sqlite table to AzureTableStorage
Requires dotnetcore-sdk 5

```
dotnet run upload -v --source store.db --sourceTable my-table --sourceTypeMap Id=guid UpdatedAt=datetime --destConnString <your-azure-table-conn-string-here> --destTableName=az-my-table
```
