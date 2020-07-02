using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Client.Extensions;
using Microsoft.Extensions.Logging;

namespace KafkaApi
{
    public class ProdutoProducer : IProdutoProducer
    {
        IKafkaClient<string, string> kafkaClient;
        ILogger<ProdutoProducer> logger;

        public ProdutoProducer(IKafkaClient<string, string> kafkaClient,
                           ILogger<ProdutoProducer> logger)
        {
            this.kafkaClient = kafkaClient;
            this.logger = logger;
        }

        public async Task<string> EnviarMensagem(string topico, string mensagem)
        {
            var msg = new Message<string, string>
            {
                Key = Guid.NewGuid().ToString(),
                Value = mensagem
            };

            var resultado = await kafkaClient.ProduceAsync(topico, msg);

            if (resultado != null)
            {
                string log = $"Delivered {resultado.Value} to {resultado.TopicPartitionOffset}";
                logger.LogInformation(log);
                return log;
            }
            else
            {
                return "Erro";
            }
        }
    }
}