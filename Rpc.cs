using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
using Google.Protobuf;
namespace SanguoDotNet;

public interface RpcCodecI 
{
    byte[] Encode(IMessage m);
    void Decode(byte[] buff,IMessage m);
}

public class RpcPbCodec : RpcCodecI
{
    public byte[] Encode(IMessage m)
    {
        MemoryStream stream = new MemoryStream();
        m.WriteTo(stream);
        return stream.ToArray();   
    }


    public void Decode(byte[] buff,IMessage m)
    {
        MemoryStream stream = new MemoryStream(buff);    
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
    void Reply(Rpc.Proto.rpcResponse response);
    LogicAddr Peer();
}

public class rpcChannel : RpcChannelI
{
    private LogicAddr _peer;
    private Node      _node;
    private Sanguo    _sanguo;

    public rpcChannel(Sanguo sanguo,Node node,LogicAddr peer)
    {
        _sanguo = sanguo;
        _node = node;
        _peer = peer;
    }

    public void SendRequest(Rpc.Proto.rpcRequest request,CancellationToken cancellationToken)
    {
        _node.SendMessage(_sanguo,new RpcRequestMessage(_peer,_sanguo.LocalAddr.LogicAddr,request),null,cancellationToken);
    }

    public void Reply(Rpc.Proto.rpcResponse response)
    {
        CancellationTokenSource source = new CancellationTokenSource();
        source.CancelAfter(5000);
        _node.SendMessage(_sanguo,new RpcResponseMessage(_peer,_sanguo.LocalAddr.LogicAddr,response),null,source.Token);
    }

    public LogicAddr Peer()
    {
        return _peer;
    }

}


public class selfChannel : RpcChannelI
{
    private Sanguo    _sanguo;

    public selfChannel(Sanguo sanguo)
    {
        _sanguo = sanguo;
    }

    public void SendRequest(Rpc.Proto.rpcRequest request,CancellationToken cancellationToken)
    {                    
        Task.Run(() =>
        {
            _sanguo.OnRpcRequest(this,request);
        });
    }

    public void Reply(Rpc.Proto.rpcResponse response)
    {
        Task.Run(() =>
        {
            _sanguo.OnRpcResponse(response);
        });
    }

    public LogicAddr Peer()
    {
        return _sanguo.LocalAddr.LogicAddr;
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

public class RpcClient
{

    public class Response<Ret>
    {
        private RpcError? _err;
        public RpcError? Err {get=>_err;}

        private Ret? _result;
        public Ret? Result {get=>_result;}
    
        public Response(Ret ret)
        {
            _result = ret;
        }

        public Response(RpcError err)
        {
            _err = err;
        }
    }

    private class callContext
    {
        private Rpc.Proto.rpcResponse? response = null;

        private SemaphoreSlim semaphore = new SemaphoreSlim(0);

        public async Task<Rpc.Proto.rpcResponse?>  Wait(CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync(cancellationToken);
            return Interlocked.Exchange(ref response,null);
        }   

        public void Wakeup(Rpc.Proto.rpcResponse resp)
        {
            Interlocked.Exchange(ref response,resp);
            semaphore.Release();
        } 
    }
    
    private ulong nextSeqno = 0;

    private Mutex mtx = new Mutex();

    private Dictionary<ulong,callContext> pendingCall = new Dictionary<ulong,callContext>();

    public void Call<Arg>(RpcChannelI channel,string method,Arg arg) where Arg : IMessage<Arg>
    {
        Rpc.Proto.rpcRequest request = new Rpc.Proto.rpcRequest();
        request.Seq = Interlocked.Add(ref nextSeqno,1);
        request.Method = method;
        request.Arg = ByteString.CopyFrom(RpcCodec.Codec.Encode(arg));
        request.Oneway = true;
        CancellationTokenSource cancel = new CancellationTokenSource();
        channel.SendRequest(request,cancel.Token);
    }

    //public Response<Ret> Call<Ret,Arg>(RpcChannelI channel,string method,Arg arg,CancellationToken cancellationToken) where Arg : IMessage<Arg> where Ret : IMessage<Ret>,new()
    //{
    //    SemaphoreSlim semaphore = new SemaphoreSlim(0);


    //}

    public async Task<Response<Ret>> CallAsync<Ret,Arg>(RpcChannelI channel,string method,Arg arg,CancellationToken cancellationToken) where Arg : IMessage<Arg> where Ret : IMessage<Ret>,new()
    {
        Rpc.Proto.rpcRequest request = new Rpc.Proto.rpcRequest();
        request.Seq = Interlocked.Add(ref nextSeqno,1);
        request.Method = method;
        request.Arg = ByteString.CopyFrom(RpcCodec.Codec.Encode(arg));
        request.Oneway = false;

        callContext context = new callContext();
        mtx.WaitOne();
        pendingCall[request.Seq] = context;
        mtx.ReleaseMutex();

        Exception? e = null;
        var cancel = false;

        Rpc.Proto.rpcResponse? resp = null;
        try{
            channel.SendRequest(request,cancellationToken);
            resp = await context.Wait(cancellationToken);
        }
        catch(OperationCanceledException)
        {
            cancel = true;
        }
        catch(Exception ee)
        {
            e = ee;
        }

        if(resp is null) {
            mtx.WaitOne();
            pendingCall.Remove(request.Seq);
            mtx.ReleaseMutex();
            if(cancel){
                //无法区分cancel原因，统一按超时处理
                return new Response<Ret>(new RpcError(RpcError.ErrTimeout,"timeout"));
            } else if (e is null) {
                throw new Exception("resp should not be null");
            } else {
                throw e;
            }
    
        } else if(resp.ErrCode == 0) {
            Ret ret = new Ret();
            RpcCodec.Codec.Decode(resp.Ret.ToByteArray(),ret);
            return new Response<Ret>(ret);
        } else {
            return new Response<Ret>(new RpcError(resp.ErrCode,resp.ErrDesc));
        }
    }

    public void OnMessage(Rpc.Proto.rpcResponse respMsg)
    {
        mtx.WaitOne();
        if(pendingCall.ContainsKey(respMsg.Seq)) {
            callContext ctx = pendingCall[respMsg.Seq];
            pendingCall.Remove(respMsg.Seq);
            mtx.ReleaseMutex();
            ctx.Wakeup(respMsg);
        } else {
            mtx.ReleaseMutex();
            Console.WriteLine($"got response but not context seqno:{respMsg.Seq}");
        }
    }
}

public class RpcServer
{
    public class Replyer<Ret> where Ret : IMessage<Ret>
    {
        private RpcChannelI _channel;
        public RpcChannelI Channel{get=>_channel;}
        private ulong       _seq;
        private int         _replied = 0;
        private bool        _oneway;

        public Replyer(RpcChannelI channel,ulong seq,bool oneway)
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

    private Mutex mtx = new Mutex();

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

    public void Register<Arg,Ret>(string method,Action<Replyer<Ret>,Arg> serviceFunc) where Arg : IMessage<Arg>,new() where Ret : IMessage<Ret>
    {

        mtx.WaitOne();
        if(methods.ContainsKey(method))
        {   
            mtx.ReleaseMutex();
            throw new Exception("duplicate method");
        }

        methods[method] = (RpcChannelI channel,Rpc.Proto.rpcRequest req) =>
        {
            try{
                Arg arg = new Arg();
                RpcCodec.Codec.Decode(req.Arg.ToByteArray(),arg);
                Replyer<Ret> replyer = new Replyer<Ret>(channel,req.Seq,req.Oneway);
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
        mtx.ReleaseMutex();
    }

    public void OnMessage(RpcChannelI channel,Rpc.Proto.rpcRequest req)
    {
        Action<RpcChannelI,Rpc.Proto.rpcRequest>? serviceFunc;
        mtx.WaitOne();
        serviceFunc = methods[req.Method];
        mtx.ReleaseMutex();
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
