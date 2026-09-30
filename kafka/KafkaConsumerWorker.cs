using System;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace PocDotNetPostgresql.kafka;

public class KafkaConsumerWorker : BackgroundService
{
    private readonly ILogger<KafkaConsumerWorker> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _topic;
    private readonly IConsumer<string, string> _consumer;

    public KafkaConsumerWorker(ILogger<KafkaConsumerWorker> logger, IConfiguration configuration) {
        _logger = logger;
        _configuration = configuration;
        _topic = _configuration.GetValue<string>("KafkaSettings:TopicName") ?? "tpc_dotnet_api";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _configuration.GetValue<string>("KafkaSettings:BootstrapServers"),
            GroupId = _configuration.GetValue<string>("KafkaSettings:GroupId"),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => StartConsumerLoop(stoppingToken), stoppingToken);
    }

    private void StartConsumerLoop(CancellationToken stoppingToken) {
        _consumer.Subscribe(_topic);
        _logger.LogInformation("Inscrito com sucesso no tópico: {Topic}", _topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested) {
                try {
                    var consumeResult = _consumer.Consume(stoppingToken);

                    if (consumeResult != null) {
                        var chave = consumeResult.Message.Key;
                        var valor = consumeResult.Message.Value;

                        _logger.LogInformation("Mensagem recebida -> Chave: {Key} | Conteúdo: {Value} | Partição: {Partition} | Offset: {Offset}",
                            chave, valor, consumeResult.Partition.Value, consumeResult.Offset.Value);
                    }
                }
                catch (ConsumeException ex) {
                    _logger.LogError("Erro ao consumir mensagem: {Reason}", ex.Error.Reason);
                }
            }


        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("O loop de consumo do Kafka foi interrompido.");
        }
        finally {
            _consumer.Close();
            _consumer.Dispose();
            _logger.LogInformation("Conexão com o Kafka encerrada graciosamente.");
        }

    }

    }





