using System;
using RabbitMQ.Client.Framing.Impl;
using RabbitMQ.Client.Exceptions;

namespace MyMQ.Util
{
    public class ConnectionWrapper : IDisposable
    {
        private Connection Connection { get; set; }
        private ChannelPool ChannelPool { get; set; }
        /// <summary>
        /// 空闲开始时间
        /// </summary>
        public DateTime IdleTime { get; private set; }

        /// <summary>
        /// 是否busy
        /// </summary>
        public bool IsBusy
        {
            get
            {
                return this.IdleTime == default(DateTime);
            }
        }

        public ConnectionWrapper(Connection connection)
        {
            if (connection == null)
            {
                throw new ArgumentNullException("connection");
            }

            this.Connection = connection;
            this.ChannelPool = new ChannelPool(this);
            this.Reusable = true;
            this.SetBusy();
        }

        /// <summary>
        /// 获取channel
        /// </summary>
        /// <returns></returns>
        public ChannelWrapper GetChannelFromPool()
        {
            ChannelWrapper channel = null;
            if (this.Reusable)
            {
                channel = this.ChannelPool.GetChannel();
                if (channel != null)
                {
                    this.SetBusy();
                }
            }

            return channel;
        }

        /// <summary>
        /// 是否有连接
        /// </summary>
        /// <returns></returns>
        public bool HasSession()
        {
            return this.Connection.m_sessionManager.Count > 0;
        }

        /// <summary>
        /// 创建新channel
        /// </summary>
        /// <returns></returns>
        public ChannelWrapper CreateChannel()
        {
            ChannelWrapper channel = null;
            if (this.CanCreateChannel())
            {
                try
                {
                    channel = new ChannelWrapper(this.Connection.CreateModel(), this);
                }
                catch (ConnectFailureException)
                {
                    this.SetUnreusable();
                }
                catch (AlreadyClosedException)
                {
                    this.SetUnreusable();
                }
                catch (OperationInterruptedException)
                {
                    this.SetUnreusable();
                }
            }

            return channel;
        }

        /// <summary>
        /// 将Connection设置为idle
        /// </summary>
        public void SetIdle()
        {
            this.IdleTime = DateTime.UtcNow;
        }

        /// <summary>
        /// 设置为busy
        /// </summary>
        private void SetBusy()
        {
            this.IdleTime = default(DateTime);
        }

        /// <summary>
        /// 设置channel不可重用
        /// </summary>
        public void SetUnreusable()
        {
            this.Reusable = false;
        }

        /// <summary>
        /// 可否可以重用
        /// 注：当connection的Reusable被标识为false时，将不再共用，等待空闲后将进入回收
        /// </summary>
        public bool Reusable { private set; get; }

        public void Dispose()
        {
            try
            {
                this.ChannelPool.Dispose();
                this.Connection.Close();
            }
            catch (Exception err)
            {
                var a = err.Message;
            }
        }


        /// <summary>
        /// 是否可创建channel
        /// </summary>
        /// <returns></returns>
        private bool CanCreateChannel()
        {
            return this.Reusable && this.Connection.m_sessionManager.Count < this.Connection.ChannelMax;
        }
    }
}
