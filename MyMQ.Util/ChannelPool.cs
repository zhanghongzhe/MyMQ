using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Timers;

namespace MyMQ.Util
{
    public class ChannelPool : IDisposable
    {
        /// <summary>
        /// 扫描间隔时间，单位（秒），默认30秒
        /// </summary>
        private const int ScanInterval = 30 * 1000;

        /// <summary>
        /// 最大闲置时间，单位（秒）,默认120秒
        /// </summary>
        private const int MaxIdleTime = 120;

        private List<ChannelWrapper> ChannelList { set; get; }

        private ConnectionWrapper Connection { set; get; }

        private readonly System.Timers.Timer ScanTimer = null;

        /// <summary>
        /// 读写锁
        /// </summary>
        private ReaderWriterLockSlim GetChannellockObj = new ReaderWriterLockSlim();

        public ChannelPool(ConnectionWrapper connection)
        {
            if (connection == null)
            {
                throw new ArgumentNullException("connection");
            }

            this.Connection = connection;
            this.ChannelList = new List<ChannelWrapper>();

            //后台清理线程，定期清理整个应用域中每一个Channel中的每一个Channel项
            ScanTimer = new System.Timers.Timer(ScanInterval);
            ScanTimer.AutoReset = true;
            ScanTimer.Enabled = true;
            ScanTimer.Elapsed += new System.Timers.ElapsedEventHandler(DoScan);
        }

        /// <summary>
        /// 从channel池中获取channel. 如果没有，创建新的channel
        /// </summary>
        /// <returns></returns>
        public ChannelWrapper GetChannel()
        {
            GetChannellockObj.EnterUpgradeableReadLock();
            try
            {
                ChannelWrapper channel = this.ChannelList.FirstOrDefault(p => p.Reusable && !p.IsBusy);
                if (channel != null)
                {
                    channel.SetBusy();
                    return channel;
                }
                else
                {
                    //创建新channel
                    GetChannellockObj.EnterWriteLock();
                    channel = this.Connection.CreateChannel();
                    if (channel != null)
                    {
                        this.ChannelList.Add(channel);
                    }
                    GetChannellockObj.ExitWriteLock();

                    return channel;
                }
            }
            finally
            {
                GetChannellockObj.ExitUpgradeableReadLock();
            }
        }

        public void Dispose()
        {
            if (this.ScanTimer != null)
            {
                this.ScanTimer.Close();
            }
            if (this.ChannelList != null)
            {
                foreach (var item in this.ChannelList)
                {
                    item.Dispose();
                }
                this.ChannelList = null;
            }
        }

        private void DoScan(object sender, ElapsedEventArgs e)
        {
            GetChannellockObj.EnterUpgradeableReadLock();

            try
            {
                var keysToRemove = new List<ChannelWrapper>();
                foreach (var item in ChannelList)
                {
                    if (!item.IsBusy && (!item.Reusable || item.IdleTime.GetDurationSeconds(DateTime.UtcNow) >= MaxIdleTime))
                    {
                        keysToRemove.Add(item);
                    }
                }

                foreach (var item in keysToRemove)
                {
                    GetChannellockObj.EnterWriteLock();
                    try
                    {
                        ChannelList.Remove(item);
                        item.Dispose();
                    }
                    finally
                    {
                        GetChannellockObj.ExitWriteLock();
                    }
                }
            }
            finally
            {
                GetChannellockObj.ExitUpgradeableReadLock();
            }
        }
    }
}
