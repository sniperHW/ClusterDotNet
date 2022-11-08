using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
using Google.Protobuf;
namespace SanguoDotNet;

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