using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using Kafka.Client.Extensions;

namespace KafkaWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private IKafkaClient<string, string> _kafkaClient;

        public Worker(
            IKafkaClient<string, string> kafkaClient, 
            ILogger<Worker> logger)
        {
            _logger = logger;
            _kafkaClient = kafkaClient;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                var resultado = _kafkaClient.Consume("produtos");

                if (resultado != null)
                {
                    _logger.LogInformation($"Consumed message: {resultado.Value} to {resultado.TopicPartitionOffset}");
                }

                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}
