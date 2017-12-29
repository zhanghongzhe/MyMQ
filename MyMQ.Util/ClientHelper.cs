using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyMQ.Util
{
    public class ClientHelper
    {
        private static object lockObj = new object();
        private static Dictionary<string, ConnectionPool> dictConnectionPool = new Dictionary<string, ConnectionPool>();

        /// <summary>
        /// 异常重试次数
        /// </summary>
        private static readonly int MaxRetryCount = 3;
        /// <summary>
        /// 等待重连时间（毫秒）
        /// </summary>
        private static readonly int RetryWaitTime = 100;

        static void ConfigFactory_OnModified(string typeName)
        {
            if (typeName == typeof(MQProviderConfig).Name.ToLower())
            {
                ClearConnectionPool();
            }
        }

        private string ConnectionConfigName
        {
            set;
            get;
        }
        private ConnectionPool CurrentConnnectionPool
        {
            get
            {
                EnsureConnectionPool();
                return dictConnectionPool[ConnectionConfigName];
            }
        }

        public ClientHelper(string connectionName)
        {
            if (String.IsNullOrEmpty(connectionName))
            {
                throw new ArgumentNullException("connectionName");
            }
            
            this.ConnectionConfigName = connectionName;
        }


        /// <summary>
        /// 发送消息
        /// </summary>
        /// <param name="exchange"></param>
        /// <param name="exchangeType"></param>
        /// <param name="routingKey"></param>
        /// <param name="message"></param>
        /// <param name="isPersistent">消息是否持久化</param>
        /// <param name="isConfirm">是否等待confirm通知</param>
        public void SendMessage(string exchange, string exchangeType, string routingKey, byte[] message, bool isPersistent = false, bool isConfirm = false)
        {
            if (String.IsNullOrEmpty(exchange))
            {
                throw new ArgumentNullException("exchange");
            }
            if (String.IsNullOrEmpty(exchangeType))
            {
                throw new ArgumentNullException("exchangeType");
            }
            if (String.IsNullOrEmpty(routingKey))
            {
                throw new ArgumentNullException("routingKey");
            }
            if (message == null || message.Length < 1)
            {
                throw new ArgumentNullException("message");
            }

            #region 发送消息。如果发送失败，自动尝试多次发送

            ChannelWrapper channel = null;
            var success = false;
            var retryCount = 0;
            Action exceptionFun = () =>
            {
                if (channel != null)
                {
                    channel.SetNotBusy();
                    channel.SetUnreusable();
                }

                retryCount++;
                Thread.Sleep(RetryWaitTime);
            };
            Action action = () =>
            {
                try
                {
                    channel = CurrentConnnectionPool.GetChannel();

                    var realChannel = channel.Channel;
                    //为了便于规范管理和定义统一，exchange、queue、binding均由专人管理，程序中不进行创建
                    //realChannel.ExchangeDeclare(exchange, exchangeType, isPersistent, false, null);
                    if (isConfirm)
                    {
                        realChannel.ConfirmSelect();
                    }

                    var properties = realChannel.CreateBasicProperties();
                    properties.SetPersistent(isPersistent);
                    realChannel.BasicPublish(exchange, routingKey, properties, message);

                    if (isConfirm)
                    {
                        realChannel.WaitForConfirmsOrDie();
                    }
                    if (channel != null)
                    {
                        channel.SetNotBusy();
                    }
                    success = true;
                }
                catch (ConnectFailureException)
                {
                    exceptionFun();
                }
                catch (AlreadyClosedException)
                {
                    exceptionFun();
                }
                catch (OperationInterruptedException)
                {
                    exceptionFun();
                }
                catch (Exception)
                {
                    exceptionFun();
                }
            };

            do
            {
                action();
            } while (!success && retryCount <= MaxRetryCount);

            #endregion

            //始终不能成功，抛出异常
            if (!success)
            {
                throw new Exception("RabbitMQ连接异常");
            }
        }

        /// <summary>
        /// 接收消息
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="isAck">是否发送ack确认</param>
        /// <returns></returns>
        public byte[] ReceiveMessage(string queue, bool isAck = false)
        {
            if (String.IsNullOrEmpty(queue))
            {
                throw new ArgumentNullException("queue");
            }

            #region 接收消息。如果接收失败，自动尝试多次接收

            byte[] message = null;
            ChannelWrapper channel = null;

            var success = false;
            var retryCount = 0;
            Action exceptionFun = () =>
            {
                if (channel != null)
                {
                    channel.SetNotBusy();
                    channel.SetUnreusable();
                }

                retryCount++;
                Thread.Sleep(RetryWaitTime);
            };
            Action action = () =>
            {
                try
                {
                    channel = CurrentConnnectionPool.GetChannel();

                    var realChannel = channel.Channel;
                    //为了便于规范管理和定义统一，exchange、queue、binding均由专人管理，程序中不进行创建
                    //realChannel.QueueDeclare(queue, true, false, false, null);

                    BasicGetResult result = realChannel.BasicGet(queue, !isAck);
                    if (result != null)
                    {
                        if (isAck)
                        {
                            realChannel.BasicAck(result.DeliveryTag, false);
                        }
                        message = result.Body;
                    }

                    channel.SetNotBusy();
                    success = true;
                }
                catch (ConnectFailureException)
                {
                    exceptionFun();
                }
                catch (AlreadyClosedException)
                {
                    exceptionFun();
                }
                catch (OperationInterruptedException)
                {
                    exceptionFun();
                }
                catch (Exception)
                {
                    exceptionFun();
                }
            };

            do
            {
                action();
            } while (!success && retryCount <= MaxRetryCount);

            #endregion

            //始终不能成功，抛出异常
            if (!success)
            {
                throw new Exception("RabbitMQ连接异常");
            }

            return message;
        }


        /// <summary>
        /// 清除连接池
        /// </summary>
        private static void ClearConnectionPool()
        {
            if (dictConnectionPool != null && dictConnectionPool.Any())
            {
                lock (lockObj)
                {
                    if (dictConnectionPool != null && dictConnectionPool.Any())
                    {
                        foreach (var kv in dictConnectionPool)
                        {
                            kv.Value.Dispose();
                        }
                        dictConnectionPool.Clear();
                    }
                }
            }
        }

        private void EnsureConnectionPool()
        {
            if (!dictConnectionPool.ContainsKey(this.ConnectionConfigName))
            {
                var mqConfig = new MQProviderConfig() { MQProviders = new List<MQProdider>() { new MQProdider() { Category = MQCategory.RabbitMQ } } };
                var mqProvider = mqConfig.MQProviders.FirstOrDefault(p => p.Category == MQCategory.RabbitMQ);
                if (mqProvider == null || mqProvider.MQConnections == null || !mqProvider.MQConnections.Any())
                {
                    throw new Exception("配置文件关键元素缺失");
                }

                var connection = mqProvider.MQConnections.FirstOrDefault(c => String.Equals(c.Name, this.ConnectionConfigName, StringComparison.InvariantCulture));
                if (connection == null)
                {
                    throw new Exception("当前连接串对应的配置不存在");
                }

                lock (lockObj)
                {
                    if (!dictConnectionPool.ContainsKey(this.ConnectionConfigName))
                    {
                        dictConnectionPool.Add(this.ConnectionConfigName, new ConnectionPool(connection));
                    }
                }
            }
        }
    }
}
