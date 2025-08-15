
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

var connection = app.Services.GetRequiredService<ISproutConnection>();
var server = app.Services.GetRequiredService<ISproutDB>();


connection.Execute("create database testdb");
connection.Execute("create table users");
connection.Execute("add column users.name string");
connection.Execute("add column users.age number");

connection.Execute("upsert users { name: 'John Doe', age: 30 }");

Console.WriteLine();