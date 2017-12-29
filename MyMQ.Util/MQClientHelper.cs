using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Framing.Impl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace MyMQ.Util
{
    public class MQClientHelper
    {
        private static object lockObj = new object();
        private ConnectionPool _connectionPool;
        protected ConnectionPool ConnectionPool
        {
            get {
                if (this._connectionPool == null)
                {
                    lock (lockObj)
                    { 
                        if (this._connectionPool == null)
                            this._connectionPool = new ConnectionPool("localhost");
                    }
                }

                return _connectionPool;
            }
        }
        
        public void Send(string msg)
        {
            var channel = this.ConnectionPool.GetChannel().Channel;
            var queueDeclare = channel.QueueDeclare("hello", false, false, false, null);
            channel.BasicPublish("", "hello", null, Encoding.UTF8.GetBytes(msg));
        }
        public void Receive()
        {
            var channel = this.ConnectionPool.GetChannel().Channel;
            var queueDeclare = channel.QueueDeclare("hello", false, false, false, null);

            channel.BasicQos(0, 1, false);
            var consumer = new RabbitMQ.Client.Events.EventingBasicConsumer(channel);
            consumer.Received += (sender, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body);
                Console.WriteLine("[x] Received {0}", message);
                channel.BasicAck(ea.DeliveryTag, false);
            };
            channel.BasicConsume(queueDeclare.QueueName, false, consumer);
        }
    }
}
