# sanguo for dotnet core

# how to use

```c#

using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Echo = ClusterTest.Proto.Rpc.Echo;
using ClusterDotNet;

namespace ClusterTest{

    class discovery : IDiscovery
    {
        private Dictionary<uint,DiscoveryNode> nodes = new Dictionary<uint,DiscoveryNode>();

        public void AddNode(DiscoveryNode node)
        {
            if(!nodes.ContainsKey(node.Addr.LogicAddr.ToUint32())){
                nodes[node.Addr.LogicAddr.ToUint32()] = node;
            }
        }

        public void RemNode(LogicAddr node)
        {
            if(nodes.ContainsKey(node.ToUint32())){
                nodes.Remove(node.ToUint32());
            }
        }


        public void Subscribe(Action<DiscoveryNode[]> onUpdate)
        {
            DiscoveryNode[] n = new DiscoveryNode[nodes.Count];
            var i = 0;
            foreach( KeyValuePair<uint,DiscoveryNode> kvp in nodes ){
                n[i++] = kvp.Value;
            }
            onUpdate(n);
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            ProtoMessage.Register("ss",1,new Proto.Echo());
            var addr = new Addr("1.1.1","127.0.0.1:8010");
            discovery d = new discovery();
            d.AddNode(new DiscoveryNode(addr,false,true));

            Console.WriteLine("server");
            ClusterNode.Instance.RegisterMsg<Proto.Echo>((LogicAddr from,Proto.Echo msg)=>{
                Console.WriteLine($"got msg echo {msg.Msg}");
                ClusterNode.Instance.SendMessage(from,msg);
            });

        
            ClusterNode.Instance.RegisterRpc("Echo",(RpcReplyer<Echo.response> replyer, Echo.request req)=>{
                Console.WriteLine($"got rpc request echo {req.Msg}");
                var resp = new Echo.response();
                resp.Msg = "this is rpc response";
                replyer.Reply(resp);
            });


            ClusterNode.Instance.Start(d,addr);
            ClusterNode.Instance.Wait();            



        }
    }
}    


```