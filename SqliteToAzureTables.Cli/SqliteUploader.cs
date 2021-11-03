using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace SqliteToAzureTables.Cli
{
    public class SqliteUploader
    {
        private readonly ILogger<SqliteUploader> _logger;

        public SqliteUploader(ILogger<SqliteUploader> logger)
        {
            _logger = logger;
        }

        public async Task Execute(ProgramOptions programOptions)
        {
            await using var dbconn = new SqliteConnection("dataSource=" + programOptions.Source.FullName);
            var schema = await dbconn.QueryAsync<SqliteSchemaResult>(@"
SELECT t.name AS tbl_name, c.name as col_name, c.type, c.""notnull"", c.dflt_value, c.pk
FROM sqlite_master AS t, pragma_table_info(t.name) AS c
WHERE t.type = 'table';");

            var schematbllookup = schema.ToLookup(x => x.tbl_name, StringComparer.OrdinalIgnoreCase);

            if (!schematbllookup.Contains(programOptions.SourceTable))
            {
                _logger.LogError("No such source table");
                return;
            }


            _logger.LogInformation("Source table schema");
            var selTblSchema = schematbllookup[programOptions.SourceTable];
            foreach (var coldef in selTblSchema)
            {
                _logger.LogInformation(
                    $"Col {coldef.col_name}; Type: {coldef.type}; Def: {coldef.dflt_value}; IsPK: {coldef.pk}");
            }

            var pkcolset = selTblSchema.Where(x => x.pk).Select(z => z.col_name).ToArray();
            var pkcolexp = string.Join(",", pkcolset);

            var tableServiceClient = new TableServiceClient(programOptions.DestConnString);
            try
            {
                var propertiesAsync = await tableServiceClient.GetPropertiesAsync();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Cannot connect to table service");
                return;
            }

            var tableClient = tableServiceClient.GetTableClient(programOptions.DestTableName);
            try
            {
                var tableresp = await tableClient.CreateIfNotExistsAsync();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Cannot create table");
                return;
            }

            var anyItem = await tableClient.QueryAsync<TableEntity>(maxPerPage: 1).AsPages().FirstOrDefaultAsync();
            if (anyItem?.Values?.Count > 0)
            {
                _logger.LogWarning("Destination table is not empty");
            }

            long offset = 0;
            while (true)
            {
                var data =
                    await dbconn.QueryAsync(
                        @$"SELECT * FROM {programOptions.SourceTable} ORDER BY {pkcolexp} LIMIT 100 OFFSET {offset}"
                    );

                var enumerated = data.ToArray();
                _logger.LogInformation($"Offset: {offset}; Got: {enumerated.Length}");

                var mapped = enumerated.Cast<IDictionary<string, object>>().Select(row =>
                {
                    var mappedRow = row.Keys.ToDictionary(
                        ks => ks,
                        es => TryConvertRowValue(row, es, programOptions.SourceTypeMap),
                        StringComparer.OrdinalIgnoreCase);
                    
                    var tent = new TableEntity(mappedRow)
                    {
                        PartitionKey = "default",
                        RowKey = TryConvertRowValue(row, pkcolexp, programOptions.SourceTypeMap)?.ToString()
                    };

                    return new TableTransactionAction(TableTransactionActionType.Add, tent);
                }).ToArray();

                var resp = await tableClient.SubmitTransactionAsync(mapped);


                if (enumerated.Length < 100)
                {
                    break;
                }

                offset += enumerated.Length;
            }
        }

        public object? TryConvertRowValue(IDictionary<string, object> row, string colname, IDictionary<string, AzureTableTypes> mappings)
        {
            if (!row.TryGetValue(colname, out var srcVal))
            {
                return null;
            }
            
            return mappings.TryGetValue(colname, out var desType) ? ConvertType(srcVal, desType) : srcVal;
        }
        
        public object ConvertType(object dbValue, AzureTableTypes destval) => destval switch
        {
            AzureTableTypes.Guid => dbValue switch
            {
                byte[] guidBytes => new Guid(guidBytes[0..16]),
                string guidString => Guid.Parse(guidString)
            },
            AzureTableTypes.DateTime => DateTime.SpecifyKind(dbValue switch
            {
                Int32 or Int64 => UnixTimeStampToDateTime((double)dbValue),
                string dateString => DateTime.Parse(dateString, CultureInfo.InvariantCulture)
            }, DateTimeKind.Utc),
            AzureTableTypes.Boolean => Convert.ToBoolean(dbValue),
            AzureTableTypes.Double => Convert.ToDouble(dbValue),
            AzureTableTypes.Int32 => Convert.ToInt32(dbValue),
            AzureTableTypes.Int64 => Convert.ToInt64(dbValue),
            _ => dbValue
        };

        public static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds(unixTimeStamp).ToLocalTime();
            return dtDateTime;
        }

        public static string ByteArrayToString(byte[] ba)
        {
            return BitConverter.ToString(ba);
        }
    }

    public class SqliteSchemaResult
    {
        public string tbl_name { get; set; }
        public string col_name { get; set; }
        public string type { get; set; }
        public string dflt_value { get; set; }
        public bool pk { get; set; }
    }
}