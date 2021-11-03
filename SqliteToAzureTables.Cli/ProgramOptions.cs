using System.Collections.Generic;
using System.IO;

namespace SqliteToAzureTables.Cli
{
    public enum AzureTableTypes
    {
        String,
        Boolean,
        Binary,
        DateTime,
        Double,
        Guid,
        Int32,
        Int64
    }
    public record ProgramOptions(
        FileInfo Source,
        string SourceTable,
        IDictionary<string, AzureTableTypes> SourceTypeMap,
        string DestConnString,
        string DestTableName
    );
}