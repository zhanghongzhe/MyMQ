using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    public class Class1
    {
        public void Receive()
        {
            var factory = new RabbitMQ.Client.ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var queueDeclare = channel.QueueDeclare(queue: "hello",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                    //在Receiver处理并确认前一个消息之前，不会分发新的消息给Receiver
                    channel.BasicQos(0, 1, false);

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (sender, ea) =>
                    {
                        var message = Encoding.UTF8.GetString(ea.Body);
                        Console.WriteLine("[x] Received {0}", message);
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    };
                    channel.BasicConsume(queue: queueDeclare.QueueName, noAck: false, consumer: consumer);
                    Console.WriteLine("consumer启动成功，Press [enter] to exit.");
                    Console.ReadLine(); //注意不要跳出，或者会释放资源，无法自动同步

                    //Console.WriteLine("Listening...");
                    //QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
                    //channel.BasicConsume("hello", false, consumer);
                    //while (true)
                    //{
                    //    //阻塞函数，获取队列中的消息
                    //    BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
                    //    byte[] bytes = ea.Body;
                    //    string str = Encoding.UTF8.GetString(bytes);
                    //    Console.WriteLine("HandleMsg:" + str);
                    //    //回复确认
                    //    channel.BasicAck(ea.DeliveryTag, false);
                    //}
                }
            }
        }
    }
}
