using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyMQ.App
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new MyMQ.Util.MQClientHelper();
            client.Send("你好");
            client.Receive();

            Console.WriteLine("complete");
            Console.ReadLine();
        }
    }
}
