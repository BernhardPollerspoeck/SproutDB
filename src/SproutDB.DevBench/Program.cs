
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SproutDB.Engine;
using SproutDB.Engine.Compilation;
using SproutDB.Engine.Execution;
using SproutDB.Engine.Parsing;



var builder = Host.CreateApplicationBuilder(args);

builder.AddSproutDB();

var app = builder.Build();

app.Start();

var parser = app.Services.GetRequiredService<IQueryParser>();
var compiler = app.Services.GetRequiredService<IQueryCompiler>();
var executor = app.Services.GetRequiredService<IQueryExecutor>();

var server = app.Services.GetRequiredService<ISproutDB>();

var createDatabaseQuery = "create database testdb";
var createDatabaseTokens = parser.Parse(createDatabaseQuery);
var createDatabaseAst = compiler.Compile(createDatabaseTokens);
executor.Execute(createDatabaseAst);

var createTableQuery = "create table users";
var createTableTokens = parser.Parse(createTableQuery);
var createTableAst = compiler.Compile(createTableTokens);
executor.Execute(createTableAst);

var addColumnQuery = "add column users.name string";
var addColumnTokens = parser.Parse(addColumnQuery);
var addColumnAst = compiler.Compile(addColumnTokens);
executor.Execute(addColumnAst);

//TODO: parsing improvement to support [TABLE].[COLUMN] syntax (for now after "Column" Token)


Console.WriteLine();