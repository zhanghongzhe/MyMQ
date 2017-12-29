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
    public class ConnectionPool : IDisposable
    {
        private string _hostName;
        private readonly List<ConnectionWrapper> connectionList = null;
        private readonly System.Timers.Timer ScanTimer = null;
        private static ReaderWriterLockSlim GetConnlockObj = new ReaderWriterLockSlim(); //读写锁
        private const int ScanInterval = 30 * 1000; //扫描周期，单位（秒），默认30秒
        private const int MaxIdleTime = 120; //最大闲置时间，单位（秒）,默认120秒
        private const int ReconnectWaitTime = 100; //等待重连时间（毫秒）
        private const int MaxReconnectCount = 3; //最大重连次数

        public ConnectionPool(string hostName)
        {
            this._hostName = hostName;

            this.ScanTimer = new System.Timers.Timer(ScanInterval);
            this.ScanTimer.AutoReset = true;
            this.ScanTimer.Enabled = true;
            this.ScanTimer.Elapsed += new System.Timers.ElapsedEventHandler(DoScan);
            this.connectionList = new List<ConnectionWrapper>();
        }

        /// <summary>
        /// 获取channel
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        public ChannelWrapper GetChannel()
        {
            GetConnlockObj.EnterUpgradeableReadLock();
            ChannelWrapper channel = null;
            try
            {
                foreach (var item in connectionList)
                {
                    channel = item.GetChannelFromPool();
                    if (channel != null)
                    {
                        return channel;
                    }
                }


                #region 创建新连接

                ConnectionFactory cf = new ConnectionFactory() { HostName = this._hostName };
                Connection newConn = null;
                var connectSuccess = true;
                var reconnectCount = 0;
                do
                {
                    try
                    {
                        newConn = (Connection)cf.CreateConnection();
                        connectSuccess = true;
                    }
                    catch (BrokerUnreachableException)
                    {
                        connectSuccess = false;
                        reconnectCount++;
                        Thread.Sleep(ReconnectWaitTime);
                    }

                } 
                while (!connectSuccess && reconnectCount <= MaxReconnectCount);

                if (newConn == null)
                {
                    throw new BrokerUnreachableException(new Exception("Broker Unreachable"));
                }

                GetConnlockObj.EnterWriteLock();
                ConnectionWrapper connWrapper = new ConnectionWrapper(newConn);
                connectionList.Add(connWrapper);
                GetConnlockObj.ExitWriteLock();

                return connWrapper.GetChannelFromPool();

                #endregion
            }
            finally
            {
                GetConnlockObj.ExitUpgradeableReadLock();
            }
        }

        public void Dispose()
        {
            if (this.ScanTimer != null)
            {
                this.ScanTimer.Dispose();
            }
            if (this.connectionList != null)
            {
                foreach (var item in connectionList)
                {
                    item.Dispose();
                }
            }
        }

        /// <summary>
        /// 清理connection
        /// </summary>
        private void DoScan(object sender, ElapsedEventArgs e)
        {
            List<ConnectionWrapper> keysToRemove = new List<ConnectionWrapper>();

            GetConnlockObj.EnterUpgradeableReadLock();
            try
            {
                foreach (var item in connectionList)
                {
                    if (!item.HasSession())
                    {
                        if (!item.Reusable)
                        {
                            keysToRemove.Add(item);
                        }
                        else
                        {
                            if (item.IsBusy)
                            {
                                item.SetIdle();
                            }
                            else
                            {
                                if (item.IdleTime.GetDurationSeconds(DateTime.UtcNow) > MaxIdleTime)
                                {
                                    keysToRemove.Add(item);
                                }
                            }
                        }
                    }
                }

                foreach (var item in keysToRemove)
                {
                    GetConnlockObj.EnterWriteLock();
                    try
                    {
                        connectionList.Remove(item);
                        item.Dispose();
                    }
                    finally
                    {
                        GetConnlockObj.ExitWriteLock();
                    }
                }
            }
            finally
            {
                GetConnlockObj.ExitUpgradeableReadLock();
            }
        }
    }
}
