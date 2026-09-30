using Microsoft.EntityFrameworkCore;
using PocDotNetPostgresql.Data;
using PocDotNetPostgresql.kafka;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

// 1. Obter as configurações do Kafka
var bootstrapServers = builder.Configuration.GetValue<string>("KafkaSettings:BootstrapServers");

var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    Acks = Acks.All // Garante maior resiliência de entrega (reconhecimento completo dos brokers)

};
// 2. Registrar o Producer como Singleton
builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    return new ProducerBuilder<string, string>(producerConfig).Build();
});

// Register PostgreSQL DbContext
builder.Services.AddDbContext<AppDbContext>(options =>
options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));

//command migration 
//dotnet tool install --global dotnet-ef --version 7.0.7
// dotnet ef migrations add InitialCreate
// dotnet ef database update


// Add services to the container.

// Registrar o consumidor em segundo plano como um Hosted Service
builder.Services.AddHostedService<KafkaConsumerWorker>();

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

// 3. Garantir o esvaziamento (Flush) dos dados pendentes ao encerrar a aplicação
app.Lifetime.ApplicationStopping.Register(() =>
{
    var producer = app.Services.GetRequiredService<IProducer<string, string>>();
    producer.Flush(TimeSpan.FromSeconds(30));
});


app.Run();
