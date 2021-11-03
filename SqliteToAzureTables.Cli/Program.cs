using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Hosting;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Configuration;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using SqliteToAzureTables.Cli;

static CommandLineBuilder BuildCommandLine()
{
    var root = new RootCommand(@"dotnet [-v]");

    root.AddGlobalOption(new Option<bool>("-v", "Verbose logging"){
        IsRequired = false
    });
    
    var uploadCmd = new Command("upload"){
        new Option<FileInfo>("--source"){
            IsRequired = true,
        },
        new Option<string>("--sourceTable"){
            IsRequired = true,
        },
        new Option<Dictionary<string, AzureTableTypes>>("--sourceTypeMap", parseArgument: argumentResult =>
        {
            try
            {
                var dict = argumentResult.Tokens
                    .Select(t => t.Value.Split('='))
                    .ToDictionary(p => p[0], p => Enum.Parse<AzureTableTypes>(p[1], true), StringComparer.OrdinalIgnoreCase);

                return dict;
            }
            catch (Exception e)
            {
                argumentResult.ErrorMessage = e.Message;
            }
            
            return null;
        }){
            IsRequired = false
        },
        new Option<string>("--destConnString"){
            IsRequired = true,
        },
        new Option<string>("--destTableName"){
            IsRequired = true,
        }
    };
    uploadCmd.Handler = CommandHandler.Create<ProgramOptions, IHost>(Run);
    
    root.AddCommand(uploadCmd);

    return new CommandLineBuilder(root);
}

static async Task Run(ProgramOptions options, IHost host)
{
    var serviceProvider = host.Services;
    var sqliteUploader = serviceProvider.GetRequiredService<SqliteUploader>();
    await sqliteUploader.Execute(options);
}

var globalTraceSwitch = new LoggingLevelSwitch();

await BuildCommandLine()
    .UseHost(_ => Host.CreateDefaultBuilder(),
        host =>
        {
            host.ConfigureLogging(ilb =>
            {
                ilb.ClearProviders();
            });
            host.UseSerilog((hbc, lc) =>
            {
                lc.MinimumLevel.Verbose();
                lc.WriteTo.Console(levelSwitch: globalTraceSwitch);
            });
            host.ConfigureServices(services =>
            {
                services.AddSingleton<SqliteUploader>();
            });
        })
    .UseMiddleware(oinv =>
    {
        if (oinv.ParseResult.HasOption("-v"))
        {
            globalTraceSwitch.MinimumLevel = LogEventLevel.Verbose;
        }
    })
    .UseDefaults()
    .Build()
    .InvokeAsync(args);