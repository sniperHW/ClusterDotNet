using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
using Google.Protobuf;
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

    private bool _die = false;
    public  bool Die{get=>_die;}

    public  static byte[] SecretKey = Encoding.ASCII.GetBytes("sanguo_2022");
    

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

    public void RegisterMsg<T>(ushort cmd,Action<LogicAddr,T> func) where T : IMessage<T>
    {
        msgManager.Register(cmd,(LogicAddr from, IMessage m)=>{
            func(from,(T)m);
        });
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

    public LogicAddr? GetAddrByType(uint tt,int num=0)
    {
        Node? node = nodeCache.GetNodeByType(tt,num);
        if(node is null) {
            return null;
        } else {
            return node.Addr.LogicAddr;
        }
    }
}