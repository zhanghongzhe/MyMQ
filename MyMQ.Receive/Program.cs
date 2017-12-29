using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyMQ.Receive
{
    class Program
    {
        static void Main(string[] args)
        {
            Receive1();
            Console.ReadLine();
        }

        #region 发送者和接收者一对一
        public static void Receive1()
        {
            var factory = new RabbitMQ.Client.ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var queueDeclare = channel.QueueDeclare("hello", false, false, false, null);

                    //平衡调度，在Receiver处理并确认前一个消息之前，不会分发新的消息给Receiver
                    channel.BasicQos(0, 1, false);

                    var consumer = new RabbitMQ.Client.Events.EventingBasicConsumer(channel);
                    consumer.Received += (sender, ea) =>
                    {
                        var message = Encoding.UTF8.GetString(ea.Body);
                        Console.WriteLine("[x] Received {0}", message);
                        channel.BasicAck(ea.DeliveryTag, false);
                    };
                    channel.BasicConsume(queueDeclare.QueueName, false, consumer);
                    Console.WriteLine("consumer启动成功，Press [enter] to exit.");
                    Console.ReadLine(); //注意不要跳出，或者会释放资源，无法自动同步
                }
            }
        }
        #endregion
        #region 发送者和接收者一对多
        public static void Receive2()
        {
            var factory = new RabbitMQ.Client.ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var queueDeclare = channel.QueueDeclare("task_queue", true, false, false, null);

                    //用于平衡调度，指定在接收端为处理完成之前不分配其他消息给该接收端
                    //如果不设置，假如有100条待处理记录，有两个接收端处理，无论两个接收端处理速度快慢，都同等分配50条
                    //如果设置该平衡调度，处理速度快的客户端会多分配
                    channel.BasicQos(0, 1, false);

                    var consumer = new RabbitMQ.Client.Events.EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] Received {0}", message);
                        channel.BasicAck(ea.DeliveryTag, false);

                        var i = Convert.ToInt32(message.Substring(message.Length - 1, 1));
                        System.Threading.Thread.Sleep((i % 2 == 1 ? 5 : 1) * 1000);
                    };
                    channel.BasicConsume(queue: queueDeclare.QueueName, noAck: false, consumer: consumer);
                    Console.WriteLine("consumer启动成功，Press [enter] to exit.");
                    Console.ReadLine(); //注意不要跳出，或者会释放资源，无法自动同步
                }
            }
        }
        #endregion
        #region 发送至路由，不用指定队列名称
        public static void Receive3()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                    var queueName = channel.QueueDeclare().QueueName;
                    channel.QueueBind(queue: queueName, exchange: "logs", routingKey: ""); //fanout类型，会忽略routingKey

                    Console.WriteLine(" [*] Waiting for logs.");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] {0}", message);
                    };
                    channel.BasicConsume(queue: queueName,
                                         noAck: false,
                                         consumer: consumer);

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
        #endregion
        #region 发送至路由，不用指定队列名称
        public static void Receive4()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "direct_logs", type: "direct");

                    var queueName = channel.QueueDeclare().QueueName;
                    channel.QueueBind(queue: queueName, exchange: "direct_logs", routingKey: "routingKey1"); //获取routingKey等于1的队列

                    Console.WriteLine(" [*] Waiting for logs.");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] {0} {1}", message, ea.RoutingKey);
                    };
                    channel.BasicConsume(queue: queueName,
                                         noAck: false,
                                         consumer: consumer);

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
        #endregion
        #region 发送至路由，不用指定队列名称
        public static void Receive5()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");

                    var queueName = channel.QueueDeclare().QueueName;
                    channel.QueueBind(queue: queueName, exchange: "topic_logs", routingKey: "*.info");

                    Console.WriteLine(" [*] Waiting for logs.");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] {0} {1}", message, ea.RoutingKey);
                    };
                    channel.BasicConsume(queue: queueName, noAck: false, consumer: consumer);

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
        #endregion
        #region 发送至路由，不用指定队列名称（Topics）
        public static void Receive6()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "rpc_queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
                channel.BasicQos(0, 1, false);
                var consumer = new RabbitMQ.Client.Events.EventingBasicConsumer(channel);
                channel.BasicConsume(queue: "rpc_queue", noAck: false, consumer: consumer);
                Console.WriteLine(" [x] Awaiting RPC requests");

                consumer.Received += (model, ea) =>
                {
                    string response = null;

                    var body = ea.Body;
                    var props = ea.BasicProperties;
                    var replyProps = channel.CreateBasicProperties();
                    replyProps.CorrelationId = props.CorrelationId;

                    try
                    {
                        var message = Encoding.UTF8.GetString(body);
                        int n = int.Parse(message);
                        Console.WriteLine(" [.] fib({0})", message);
                        response = "返回" + message;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(" [.] " + e.Message);
                        response = "";
                    }
                    finally
                    {
                        var responseBytes = Encoding.UTF8.GetBytes(response);
                        channel.BasicPublish(exchange: "", routingKey: props.ReplyTo, basicProperties: replyProps, body: responseBytes);
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                };

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
        #endregion
    }
}
