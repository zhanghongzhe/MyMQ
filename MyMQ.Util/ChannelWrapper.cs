using System;
using RabbitMQ.Client;

namespace MyMQ.Util
{
    public class ChannelWrapper : IDisposable
    {
        private ChannelPool ChannelPool { set; get; }
        private ConnectionWrapper Connection { set; get; }
        /// <summary>
        /// 是否在工作
        /// </summary>
        public bool IsBusy { get; private set; }

        /// <summary>
        /// 空闲开始时间
        /// </summary>
        public DateTime IdleTime { get; private set; }

        /// <summary>
        /// 可否可以重用
        /// 注：当channel的Reusable被标识为false时，将不再共用，等待空闲后将进入回收
        /// </summary>
        public bool Reusable { private set; get; }

        public IModel Channel { get; private set; }

        public ChannelWrapper(IModel channel, ConnectionWrapper connection)
        {
            if (channel == null)
            {
                throw new ArgumentNullException("channel");
            }
            if (channel == connection)
            {
                throw new ArgumentNullException("connection");
            }

            this.Channel = channel;
            this.Connection = connection;
            this.Reusable = true;
            this.SetBusy();
        }

        /// <summary>
        /// 设置channel空闲
        /// </summary>
        public void SetNotBusy()
        {
            this.IsBusy = false;
            this.IdleTime = DateTime.UtcNow;
        }

        /// <summary>
        /// 设置channel空闲
        /// </summary>
        public void SetBusy()
        {
            this.IsBusy = true;
        }

        /// <summary>
        /// 设置channel不可重用
        /// </summary>
        public void SetUnreusable()
        {
            this.Reusable = false;
            this.Connection.SetUnreusable();
        }

        public void Dispose()
        {
            try
            {
                this.Channel.Dispose();
            }
            catch
            {
            }
        }
    }
}
