using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace MyMQ.Util
{
    public class MQProviderConfig
    {
        public List<MQProdider> MQProviders { set; get; }
    }

    public class MQProdider
    {
        [XmlAttribute(AttributeName = "category")]
        public MQCategory Category { set; get; }

        public List<MQConnnection> MQConnections { set; get; }
    }
    public enum MQCategory
    {
        RabbitMQ = 0
    }
    public class MQConnnection
    {
        [XmlAttribute(AttributeName = "name")]
        public string Name { set; get; }

        [XmlAttribute(AttributeName = "connectionString")]
        public string ConnnectionString { set; get; }

        [XmlAttribute(AttributeName = "connectTimeout")]
        public int ConnectTimeout { set; get; }

        /// <summary>
        /// 连接心跳超时时间(毫秒）
        /// </summary>
        [XmlAttribute(AttributeName = "heartbeatTimeout")]
        public int HeartbeatTimeout { set; get; }

        /// <summary>
        /// 重连最大次数
        /// </summary>
        [XmlAttribute(AttributeName = "reconnectMaxCount")]
        public int ReconnectMaxCount { set; get; }

        /// <summary>
        /// 重连等待时间(毫秒）
        /// </summary>
        [XmlAttribute(AttributeName = "reconnectWaitTime")]
        public int ReconnectWaitTime { set; get; }

        /// <summary>
        /// 单个连接最大的channel数
        /// </summary>
        [XmlAttribute(AttributeName = "channelMaxCount")]
        public int ChannelMaxCount { set; get; }
    }
}
