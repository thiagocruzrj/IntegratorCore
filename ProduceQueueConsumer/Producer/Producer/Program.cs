using RabbitMQ.Client;
using System;
using System.Text;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "testQueue",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                int count = 0;
                while (true)
                {

                    string message = $"{count++} Hello world";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                                         routingKey: "testQueue",
                                         basicProperties: null,
                                         body: body);
                    Console.WriteLine(" [x] Send {0}", message);
                    System.Threading.Thread.Sleep(200);
                }
            }
        }
    }
}