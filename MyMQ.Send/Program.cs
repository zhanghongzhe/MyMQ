using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyMQ.Send
{
    class Program
    {
        static void Main(string[] args)
        {
            Send1();
            Console.ReadLine();
        }
        #region 发送者和接收者一对一（"Hello World!"）
        public static void Send1()
        {
            var factory = new RabbitMQ.Client.ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var queueDeclare = channel.QueueDeclare("hello", false, false, false, null);

                    string message = "Hello World!";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish("", "hello", null, body);
                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
        #endregion
        #region 发送者和接收者一对多（Work queues）
        public static void Send2()
        {
            var factory = new RabbitMQ.Client.ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    //druable参数，用于持久化，如果为true，重启后，数据还在
                    //如果修改：如果为false重启服务可修改，如果true，需要进入到控制台，删除该列队
                    //注意：即便是设置为持久化，也有失去数据的可能，在接收到数据，写入磁盘之前，也有可能出现中断的情况
                    var queueDeclare = channel.QueueDeclare("task_queue", true, false, false, null);
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true; //配合QueueDeclare的druable参数使用，指定消息是否是持久化，如果为false，消息也不会被持久化

                    for (var i = 0; i < 10; i++)
                    {
                        var body = Encoding.UTF8.GetBytes("Messsage" + i);

                        channel.BasicPublish(exchange: "",
                                             routingKey: "task_queue",
                                             basicProperties: properties,
                                             body: body);
                        Console.WriteLine(" [x] Sent {0}", "Messsage" + i);
                    }
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
        #endregion
        #region 发送至路由，不用指定队列名称（Publish/Subscribe）
        public static void Send3()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "logs", type: "fanout"); //direct, topic, headers and fanout
                    
                    for (var i = 0; i < 10; i++)
                    {
                        var body = Encoding.UTF8.GetBytes("Messsage" + i);
                        channel.BasicPublish(exchange: "logs",
                                             routingKey: "",
                                             basicProperties: null,
                                             body: body);
                        Console.WriteLine(" [x] Sent {0}", "Messsage" + i);
                    }
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
        #endregion
        #region 发送至路由，不用指定队列名称（Routing）
        public static void Send4()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "direct_logs", type: "direct"); //direct, topic, headers and fanout

                    for (var i = 0; i < 10; i++)
                    {
                        var body = Encoding.UTF8.GetBytes("Messsage" + i);
                        channel.BasicPublish(exchange: "direct_logs",
                                             routingKey: "routingKey" + (i % 3).ToString(),
                                             basicProperties: null,
                                             body: body);
                        Console.WriteLine(" [x] Sent {0}", "Messsage" + i);
                    }
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
        #endregion
        #region 发送至路由，不用指定队列名称（Topics）
        public static void Send5()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "topic_logs", type: "topic"); //direct, topic, headers and fanout

                    for (var i = 0; i < 10; i++)
                    {
                        var body = Encoding.UTF8.GetBytes("Messsage" + i);
                        channel.BasicPublish(exchange: "topic_logs",
                                             routingKey: "anonymous.info",
                                             basicProperties: null,
                                             body: body);
                        Console.WriteLine(" [x] Sent {0}", "Messsage" + i);
                    }
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
        #endregion
        #region 发送至路由，不用指定队列名称（Topics）
        public static void Send6()
        {
            var rpcClient = new RpcClient();

            Console.WriteLine(" [x] Requesting fib(30)");
            var response = rpcClient.Call("30");

            Console.WriteLine(" [.] Got '{0}'", response);
            rpcClient.Close();
        }
        #endregion
    }
}
