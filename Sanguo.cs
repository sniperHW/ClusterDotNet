using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
using Google.Protobuf;
using System.Text.Json;
namespace SanguoDotNet;


public class SSLoginReq {
	private uint _logicAddr;
    public uint LogicAddr{get=>_logicAddr;}
	private string _netAddr;
    public string NetAddr{get=>_netAddr;}
    private bool _isStream;
    public bool IsStream{get=>_isStream;}

    public SSLoginReq(uint logicAddr,string netAddr,bool isStream){
        _logicAddr = logicAddr;
        _netAddr = netAddr;
        _isStream = isStream;
    }
}

public class Sanguo
{
    private class MsgManager 
    {
        private Mutex mtx = new Mutex();

        private Dictionary<ushort,Action<LogicAddr,IMessage>> handlers = new Dictionary<ushort,Action<LogicAddr,IMessage>>();

        public void Register(ushort cmd,Action<LogicAddr,IMessage> func)
        {
            mtx.WaitOne();
            handlers[cmd] = func;
            mtx.ReleaseMutex();
        }

        public Action<LogicAddr,IMessage>? GetHandler(ushort cmd)
        {
            Action<LogicAddr,IMessage>? handler = null;
            mtx.WaitOne();
            handler = handlers[cmd];
            mtx.ReleaseMutex();
            return handler;            
        }
    }

    private Addr _localAddr;
    public Addr LocalAddr{get=>_localAddr;}
    private NodeCache nodeCache;
    private RpcClient rpcCli = new RpcClient();
    private RpcServer rpcSvr = new RpcServer();
    private MsgManager msgManager = new MsgManager();
    private int _die = 0;
    public  bool Die{get=>_die==1;}
    public  static byte[] SecretKey = Encoding.ASCII.GetBytes("sanguo_2022");
    private SemaphoreSlim waitStop = new SemaphoreSlim(0);
    private Socket? listener;
    private int started;
    private CancellationTokenSource cancel = new CancellationTokenSource(); 
    public Node? GetNodeByLogicAddr(LogicAddr addr) 
    {
        if(addr.Cluster() == _localAddr.LogicAddr.Cluster()) 
        {
            //同一cluster
            return nodeCache.GetNodeByLogicAddr(addr);
        } else {
            //不同cluster,获取本cluster内的harbor
            if(_localAddr.LogicAddr.Type() == LogicAddr.HarbarType) {
                //当前节点为harbor,从harbor集群中获取与addr在同一个cluster的harbor节点
                return nodeCache.GetHarborByCluster(addr.Cluster(),addr);
            } else {
                //当前节点非harbor,获取集群中的harbor节点
                return nodeCache.GetHarborByCluster(_localAddr.LogicAddr.Cluster(),addr); 
            }
        }
    }

    public void RegisterMsg<T>(Action<LogicAddr,T> func) where T : IMessage<T>,new()
    {
        try{
            var cmd = ProtoMessage.GetID("ss",new T());
            msgManager.Register((ushort)cmd,(LogicAddr from, IMessage m)=>{
                func(from,(T)m);
            });
        }
        catch(Exception)
        {
            Console.WriteLine("invaild msg");
        }
    }

    public void DispatchMsg(SSMessage m)
    {
        Action<LogicAddr,IMessage>? handler = msgManager.GetHandler(m.Cmd);
        if(!(handler is null))
        {
            handler(m.From,m.Payload);
        }
    }

    public void OnRpcResponse(Rpc.Proto.rpcResponse resp)
    {
        rpcCli.OnMessage(resp);
    }   

    public void OnRpcRequest(RpcChannelI channel, Rpc.Proto.rpcRequest req)
    {
        rpcSvr.OnMessage(channel,req);
    }

    public Sanguo(Addr addr)
    {
        _localAddr = addr;
        nodeCache = new NodeCache(addr.LogicAddr);
    }
    private async Task<bool> onNewConnection(Socket s) 
    {
        var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(1000);
        try{
            using NetworkStream nstream = new(s, ownsSocket: false);
            /*
             *  以下两个read读取的字节数量很小，不可能一次性读取不全，如果发生这样的情况，当作失败处理
             *  如日后发生改变需要读取大量字节再作调整
             */    
            var head = new byte[4];    

            var n = await nstream.ReadAsync(head,0,head.Length,cancellation.Token);
            if(n < head.Length) {
                return false;
            }

            var data = new byte[IPAddress.NetworkToHostOrder(BitConverter.ToInt32(head, 0))];

            n = await nstream.ReadAsync(data,0,data.Length,cancellation.Token);
            if(n < data.Length) {
                return false;
            }

            var jsonstream = new MemoryStream(AES.CbcDecrypt(SecretKey,data));
            var ret = JsonSerializer.Deserialize(jsonstream,typeof(SSLoginReq));
            if(ret == null || !(ret is SSLoginReq))
            {
                return false;
            }

            var loginReq = (SSLoginReq)ret;

            Node? node = nodeCache.GetNodeByLogicAddr(new LogicAddr(loginReq.LogicAddr));
            if(node == null || node.Addr.NetAddr != loginReq.NetAddr)
            {
                return false;
            }

            if(loginReq.IsStream) 
            {
                await nstream.WriteAsync(BitConverter.GetBytes(0),0,sizeof(uint),cancellation.Token);
            }
            else 
            {
                if(!node.CheckConnection(this))
                {
                    return false;
                }
                await nstream.WriteAsync(BitConverter.GetBytes(0),0,sizeof(uint),cancellation.Token);
            }

            node.OnEstablish(this,s);
            return true;
        }
        catch(Exception)
        {
            return false;
        }
    }

    public void Start(DiscoveryI discovery)
    {
        if(Interlocked.CompareExchange(ref started,1,0) == 0){
            try{
                discovery.Subscribe((DiscoveryNode[] nodeInfo)=>{
                    nodeCache.onNodeUpdate(this,nodeInfo);
                });
                nodeCache.WaitInit(cancel.Token);

                if(nodeCache.GetNodeByLogicAddr(LocalAddr.LogicAddr) == null)
                {
                    Console.WriteLine($"{LocalAddr.LogicAddr.ToString()} not in config");
                    return;
                }

                listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                listener.Bind(LocalAddr.IPEndPoint());
                listener.Listen(int.MaxValue);

                Console.WriteLine($"server start at {LocalAddr.IPEndPoint()}");

                Task.Run(async ()=>{
                    try{    
                        for(;;){
                            Socket ts = await listener.AcceptAsync(cancel.Token);
                            Console.WriteLine("on new client");
                            #pragma warning disable CS4014
                            Task.Run(async ()=>{
                                var ok = await onNewConnection(ts); 
                                if(!ok) {
                                    ts.Close();
                                }
                            });
                            #pragma warning restore CS4014
                        }
                    }
                    catch(Exception)
                    {
                        return;
                    }
                });
            }
            catch(Exception e) 
            {
                Console.WriteLine(e);
                return;
            }
        }
    }

    public void Stop()
    {
        if(Interlocked.CompareExchange(ref _die,1,0) == 0){
            cancel.Cancel();
            listener?.Close();
            nodeCache.Stop();
            waitStop.Release();
        }
    }

    public void Wait() 
    {
        waitStop.Wait();
    }

    public LogicAddr? GetAddrByType(uint tt,int num=0)
    {
        Node? node = nodeCache.GetNodeByType(tt,num);
        if(node is null) {
            return null;
        } else {
            return node.Addr.LogicAddr;
        }
    }

    public void SendMessage(LogicAddr to,IMessage msg) 
    {
        Node? node = nodeCache.GetNodeByLogicAddr(to);
        if(!(node is null)){
            node.SendMessage(this,new SSMessage(to,LocalAddr.LogicAddr,msg),DateTime.Now.AddMilliseconds(1000),null);
        } else {
            Console.WriteLine($"{to.ToString} not in config");
        }
    }

    //单向调用，不接收返回值
    public void Call<Arg>(LogicAddr to,string method,Arg arg) where Arg : IMessage<Arg>
    {
        if(to == LocalAddr.LogicAddr){
            rpcCli.Call<Arg>(new selfChannel(this),method,arg);
        } else {
            Node? node = nodeCache.GetNodeByLogicAddr(to);
            if(!(node is null)) {
                rpcCli.Call<Arg>(new rpcChannel(this,node,to),method,arg);
            }
        }
    }

    public RpcClient.Response<Ret> Call<Ret,Arg>(LogicAddr to,string method,Arg arg,CancellationToken cancellationToken) where Arg : IMessage<Arg> where Ret : IMessage<Ret>,new()
    {
        if(to == LocalAddr.LogicAddr){
            return rpcCli.Call<Ret,Arg>(new selfChannel(this),method,arg,cancellationToken);
        } else {
            Node? node = nodeCache.GetNodeByLogicAddr(to);
            if(!(node is null)) {
                return rpcCli.Call<Ret,Arg>(new rpcChannel(this,node,to),method,arg,cancellationToken);
            } else {
                return new RpcClient.Response<Ret>(new RpcError(RpcError.ErrOther,"can't find target node"));
            }
        }        
    }

    public async Task<RpcClient.Response<Ret>> CallAsync<Ret,Arg>(LogicAddr to,string method,Arg arg,CancellationToken cancellationToken) where Arg : IMessage<Arg> where Ret : IMessage<Ret>,new()
    {
        if(to == LocalAddr.LogicAddr){
            return await rpcCli.CallAsync<Ret,Arg>(new selfChannel(this),method,arg,cancellationToken);
        } else {
            Node? node = nodeCache.GetNodeByLogicAddr(to);
            if(!(node is null)) {
                return await rpcCli.CallAsync<Ret,Arg>(new rpcChannel(this,node,to),method,arg,cancellationToken);
            } else {
                return new RpcClient.Response<Ret>(new RpcError(RpcError.ErrOther,"can't find target node"));
            }
        }        
    }

    public void RegisterRpc<Arg,Ret>(string method,Action<RpcServer.Replyer<Ret>,Arg> serviceFunc) where Arg : IMessage<Arg>,new() where Ret : IMessage<Ret>
    {
        rpcSvr.Register<Arg,Ret>(method,serviceFunc);
    }
}