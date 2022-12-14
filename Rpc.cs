using System;
//using System.Net;
//using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
using Google.Protobuf;
namespace ClusterDotNet;

public interface RpcCodecI 
{
    byte[] Encode(IMessage m);
    void Decode(byte[] buff,IMessage m);
}

public class RpcPbCodec : RpcCodecI
{
    public byte[] Encode(IMessage m)
    {
        using MemoryStream stream = new MemoryStream();
        m.WriteTo(stream);
        return stream.ToArray();   
    }


    public void Decode(byte[] buff,IMessage m)
    {
        using MemoryStream stream = new MemoryStream(buff);    
        m.MergeFrom(stream);
    }
}

public class RpcCodec
{
    public static RpcCodecI Codec = new RpcPbCodec();
}


public interface RpcChannelI
{
    void SendRequest(Rpc.Proto.rpcRequest request,CancellationToken cancellationToken);
    void SendRequest(Rpc.Proto.rpcRequest request,DateTime deadline);
    void Reply(Rpc.Proto.rpcResponse response);
    LogicAddr Peer();
}

internal class rpcChannel : RpcChannelI
{
    private LogicAddr _peer;
    private Node      _node;
    private ClusterNode  _self;

    public rpcChannel(ClusterNode self,Node node,LogicAddr peer)
    {
        _self = self;
        _node = node;
        _peer = peer;
    }

    public void SendRequest(Rpc.Proto.rpcRequest request,CancellationToken cancellationToken)
    {
        _node.SendMessage(_self,new RpcRequestMessage(_peer,_self.LocalAddr.LogicAddr,request),null,cancellationToken);
    }

    public void SendRequest(Rpc.Proto.rpcRequest request,DateTime deadline)
    {
        _node.SendMessage(_self,new RpcRequestMessage(_peer,_self.LocalAddr.LogicAddr,request),deadline,null);
    }


    public void Reply(Rpc.Proto.rpcResponse response)
    {
        _node.SendMessage(_self,new RpcResponseMessage(_peer,_self.LocalAddr.LogicAddr,response),DateTime.Now.AddMilliseconds(5000),null);
    }

    public LogicAddr Peer()
    {
        return _peer;
    }

}


internal class selfChannel : RpcChannelI
{
    private ClusterNode  _self;

    public selfChannel(ClusterNode self)
    {
        _self = self;
    }

    public void SendRequest(Rpc.Proto.rpcRequest request,CancellationToken cancellationToken)
    {                    
        Task.Run(() =>
        {
            _self.OnRpcRequest(this,request);
        });
    }

    public void SendRequest(Rpc.Proto.rpcRequest request,DateTime deadline)
    {
        Task.Run(() =>
        {
            _self.OnRpcRequest(this,request);
        });
    }

    public void Reply(Rpc.Proto.rpcResponse response)
    {
        Task.Run(() =>
        {
            _self.OnRpcResponse(response);
        });
    }

    public LogicAddr Peer()
    {
        return _self.LocalAddr.LogicAddr;
    }

}


public class RpcError
{

    public static readonly uint ErrOk = 0;
	public static readonly uint ErrInvaildMethod = 1;
	public static readonly uint ErrServerPause = 2;
	public static readonly uint ErrTimeout = 3;
	public static readonly uint ErrSend = 4;
	public static readonly uint ErrCancel = 5;
	public static readonly uint ErrMethod = 6;
	public static readonly uint ErrOther = 7;


    private uint _code = 0;
    public uint Code{get=>_code;}

    private string? _desc = null;
    public string? Desc{get=>_desc;}

    public RpcError(uint code,string desc)
    {
        _code = code;   
        _desc = desc;
    }
}


public class RpcException : Exception
{
    public string Msg{get;}
    
    public uint  ErrCode{get;} 

    public RpcException(string msg,uint errCode)
    {
        Msg = msg;
        ErrCode = errCode;
    }

    override public string ToString()
    {
        return $"RpcException:{Msg}";
    }
}

public class RpcResponse<Ret>
{
    public RpcError? Err {get;}
    public Ret? Result {get;}

    public RpcResponse(Ret ret)
    {
        Result = ret;
    }

    public RpcResponse(RpcError err)
    {
        Err = err;
    }
}

internal class RpcClient
{
    private class callContext
    {
        private Rpc.Proto.rpcResponse? _response = null;

        public  Rpc.Proto.rpcResponse? Response{get=> Interlocked.Exchange(ref _response,_response);}

        public ulong Seq{get;}

        private SemaphoreSlim semaphore = new SemaphoreSlim(0);

        public Task WaitAsync(CancellationToken cancellationToken)
        {
            return semaphore.WaitAsync(cancellationToken);
        }

        public void Wait(CancellationToken cancellationToken)
        {
            semaphore.Wait(cancellationToken);
        }   

        public void OnResponse(Rpc.Proto.rpcResponse resp)
        {
            Interlocked.Exchange(ref _response,resp);
            semaphore.Release();
        } 

        public callContext(ulong seq) {
            Seq = seq;
        }
    }
    
    private ulong nextSeqno = 0;

    private readonly object mtx = new object();

    private Dictionary<ulong,callContext> pendingCall = new Dictionary<ulong,callContext>();

    private Rpc.Proto.rpcRequest makeRequest<Arg>(string method,Arg arg,bool oneway) where Arg : IMessage<Arg>
    {
        Rpc.Proto.rpcRequest request = new Rpc.Proto.rpcRequest();
        request.Seq = Interlocked.Add(ref nextSeqno,1);
        request.Method = method;
        request.Arg = ByteString.CopyFrom(RpcCodec.Codec.Encode(arg));
        request.Oneway = oneway;
        return request;
    }

    internal void Call<Arg>(RpcChannelI channel,string method,Arg arg) where Arg : IMessage<Arg>
    {
        channel.SendRequest(makeRequest<Arg>(method,arg,true),DateTime.Now.AddMilliseconds(1000));
    }

    internal Ret Call<Ret,Arg>(RpcChannelI channel,string method,Arg arg,CancellationToken cancellationToken) where Arg : IMessage<Arg> where Ret : IMessage<Ret>,new()
    {
        var request = makeRequest<Arg>(method,arg,false);
        callContext context = new callContext(request.Seq);
        lock(mtx)
        {
            pendingCall[request.Seq] = context;
        }
        try{
            channel.SendRequest(request,cancellationToken);
            context.Wait(cancellationToken);
        }
        catch(Exception)
        {
            lock(mtx)
            {
                pendingCall.Remove(context.Seq);
            }
            throw;
        }

        if (context.Response is null)
        {
            throw new Exception("resp should not be null");
        } 
        else 
        {
            var resp = context.Response;
            if(resp.ErrCode == 0) {
                Ret ret = new Ret();
                RpcCodec.Codec.Decode(resp.Ret.ToByteArray(),ret);
                return ret;
            } else {
                throw new RpcException(resp.ErrDesc,resp.ErrCode);
            }
        }
    }

    internal async Task<Ret> CallAsync<Ret,Arg>(RpcChannelI channel,string method,Arg arg,CancellationToken cancellationToken) where Arg : IMessage<Arg> where Ret : IMessage<Ret>,new()
    {
        var request = makeRequest<Arg>(method,arg,false);
        callContext context = new callContext(request.Seq);
        lock(mtx)
        {
            pendingCall[request.Seq] = context;
        }

        try{
            channel.SendRequest(request,cancellationToken);
            await context.WaitAsync(cancellationToken);
        } 
        catch(Exception)
        {
            lock(mtx)
            {
                pendingCall.Remove(context.Seq);
            }
            throw;
        }

        if (context.Response is null)
        {
            throw new Exception("resp should not be null");
        } 
        else 
        {
            var resp = context.Response;
            if(resp.ErrCode == 0) {
                Ret ret = new Ret();
                RpcCodec.Codec.Decode(resp.Ret.ToByteArray(),ret);
                return ret;
            } else {
                throw new RpcException(resp.ErrDesc,resp.ErrCode);
            }
        }
    }

    internal void OnMessage(Rpc.Proto.rpcResponse respMsg)
    {
        lock(mtx)
        {
            if(pendingCall.ContainsKey(respMsg.Seq)) {
                callContext ctx = pendingCall[respMsg.Seq];
                pendingCall.Remove(respMsg.Seq);
                ctx.OnResponse(respMsg);
            } else {
                Console.WriteLine($"got response but not context seqno:{respMsg.Seq}");
            }
        }
    }
}

public class RpcReplyer<Ret> where Ret : IMessage<Ret>
{
    private RpcChannelI _channel;
    public RpcChannelI Channel{get=>_channel;}
    private ulong       _seq;
    private int         _replied = 0;
    private bool        _oneway;

    public RpcReplyer(RpcChannelI channel,ulong seq,bool oneway)
    {
        _channel = channel;
        _seq = seq;
        _oneway = oneway;
    }

    public void Reply(Ret ret)
    {
        if(!_oneway && Interlocked.CompareExchange(ref _replied,1,0) == 0)
        {
            Rpc.Proto.rpcResponse response = new Rpc.Proto.rpcResponse();

            response.Seq = _seq;

            response.Ret = ByteString.CopyFrom(RpcCodec.Codec.Encode(ret));

            _channel.Reply(response);
        }
    }

    public void Reply(RpcError error)
    {
        if(!_oneway && Interlocked.CompareExchange(ref _replied,1,0) == 0)
        {
            Rpc.Proto.rpcResponse response = new Rpc.Proto.rpcResponse();

            response.Seq = _seq;

            response.ErrCode = error.Code;

            response.ErrDesc = error.Desc;

            _channel.Reply(response);

        }
    }
}

internal class RpcServer
{
    private readonly object mtx = new object();

    private Dictionary<string,Action<RpcChannelI,Rpc.Proto.rpcRequest>> methods = new Dictionary<string,Action<RpcChannelI,Rpc.Proto.rpcRequest>>();

    private int pause = 0;

    public void Pause()
    {
        Interlocked.CompareExchange(ref pause,1,0);
    }

    public void Resume()
    {
        Interlocked.CompareExchange(ref pause,0,1);
    }

    public void Register<Arg,Ret>(string method,Action<RpcReplyer<Ret>,Arg> serviceFunc) where Arg : IMessage<Arg>,new() where Ret : IMessage<Ret>
    {
        lock(mtx) 
        {
            if(methods.ContainsKey(method))
            {   
                throw new ClusterException("duplicate rpc method");
            } else {
                methods[method] = (RpcChannelI channel,Rpc.Proto.rpcRequest req) =>
                {
                    try{
                        Arg arg = new Arg();
                        RpcCodec.Codec.Decode(req.Arg.ToByteArray(),arg);
                        RpcReplyer<Ret> replyer = new RpcReplyer<Ret>(channel,req.Seq,req.Oneway);
                        serviceFunc(replyer,arg);
                    }catch(Exception e)
                    {
                        Console.WriteLine(e);
                        if(!req.Oneway){
                            Rpc.Proto.rpcResponse resp = new Rpc.Proto.rpcResponse();
                            resp.Seq = req.Seq;
                            resp.ErrCode = RpcError.ErrMethod;
                            resp.ErrDesc = e.ToString();
                            channel.Reply(resp);
                        }    
                    }
                };                
            }
        }
    }

    public void OnMessage(RpcChannelI channel,Rpc.Proto.rpcRequest req)
    {
        Action<RpcChannelI,Rpc.Proto.rpcRequest>? serviceFunc;
        lock(mtx)
        {
            serviceFunc = methods[req.Method];
        }
        
        if(serviceFunc is null){
            Rpc.Proto.rpcResponse resp = new Rpc.Proto.rpcResponse();
            resp.Seq = req.Seq;
            resp.ErrCode = RpcError.ErrMethod;
            resp.ErrDesc = "invaild method";
            channel.Reply(resp);
        } 
        else if (pause == 1)
        {
            Rpc.Proto.rpcResponse resp = new Rpc.Proto.rpcResponse();
            resp.Seq = req.Seq;
            resp.ErrCode = RpcError.ErrServerPause;
            resp.ErrDesc = "server pause";
            channel.Reply(resp);            
        } else {
            serviceFunc(channel,req);
        }
    }
}
