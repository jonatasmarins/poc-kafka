using System.Threading.Tasks;
using KafkaApi.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace KafkaApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ProdutoController : ControllerBase
    {
        private readonly ILogger<ProdutoController> _logger;
        private readonly IProdutoProducer _producer;

        public ProdutoController(IProdutoProducer producer, ILogger<ProdutoController> logger)
        {
            _logger = logger;
            _producer = producer;
        }

        [HttpPost]
        public async Task<ActionResult> Post(Produto produto)
        {
            string mensagem = JsonConvert.SerializeObject(produto);
            var resultado = await _producer.EnviarMensagem("produtos", mensagem);

            return Ok(resultado);
        }
    }
}