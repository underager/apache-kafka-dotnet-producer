using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using ProducerApplication.Models;

namespace ProducerApplication.Controllers
{
    public class CarsController : ControllerBase
    {
        private ProducerConfig _configuration;
        private readonly IConfiguration _config;

        public CarsController(ProducerConfig configuration, IConfiguration config)
        {
            _configuration = configuration; 
            _config = config;
        }

        [HttpGet]
        public string Ping()
        {
            return "Cars Pinged";
        }

        [HttpPost("sendBookingDetails")]
        public async Task<ActionResult> Get([FromBody] CarDetails car)
        {
            var topic = _config.GetSection("TopicName").Value;

            string serializedData = Newtonsoft.Json.JsonConvert.SerializeObject(car);

            using(var producer = new ProducerBuilder<Null, string>(_configuration).Build())
            {
                await producer.ProduceAsync(topic, new Message<Null, string> { Value = serializedData });
                producer.Flush(TimeSpan.FromSeconds(10));

                return Ok(true);

            }
        }

    }
}
