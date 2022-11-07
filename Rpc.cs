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
    void SendRequest(Rpc.Proto.rpcRequest request);
    void Reply(Rpc.Proto.rpcResponse response);
}


/*

type Client struct {
	nextSequence uint64
	codec        Codec
	pendingCall  [32]sync.Map
}

func NewClient(codec Codec) *Client {
	return &Client{
		codec: codec,
	}
}

func (c *Client) OnMessage(context context.Context, resp *ResponseMsg) {
	if ctx, ok := c.pendingCall[int(resp.Seq)%len(c.pendingCall)].LoadAndDelete(resp.Seq); ok {
		ctx.(*callContext).stopTimer()
		ctx.(*callContext).callOnResponse(c.codec, resp.Ret, resp.Err)
	} else {
		logger.Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (c *Client) CallWithCallback(channel Channel, deadline time.Time, method string, arg interface{}, ret interface{}, respCb RespCB) func() bool {
	if b, err := c.codec.Encode(arg); err != nil {
		logger.Panicf("encode error:%v", err)
		return nil
	} else {
		reqMessage := &RequestMsg{
			Seq:    atomic.AddUint64(&c.nextSequence, 1),
			Method: method,
			Arg:    b,
		}
		if respCb == nil || ret == nil {
			reqMessage.Oneway = true
			if err = channel.SendRequest(reqMessage, deadline); err != nil {
				logger.Errorf("SendRequest error:%v", err)
			}
			return nil
		} else {

			ctx := &callContext{
				onResponse:   respCb,
				respReceiver: ret,
			}

			pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]

			pending.Store(reqMessage.Seq, ctx)

			ctx.deadlineTimer.Store(time.AfterFunc(deadline.Sub(time.Now()), func() {
				if _, ok := pending.LoadAndDelete(reqMessage.Seq); ok {
					ctx.callOnResponse(c.codec, nil, newError(ErrTimeout, "timeout"))
				}
			}))

			if err = channel.SendRequest(reqMessage, deadline); err != nil {
				if _, ok := pending.LoadAndDelete(reqMessage.Seq); ok {
					ctx.stopTimer()
					if e, ok := err.(net.Error); ok && e.Timeout() {
						go ctx.callOnResponse(c.codec, nil, newError(ErrTimeout, "timeout"))
					} else {
						go ctx.callOnResponse(c.codec, nil, newError(ErrSend, err.Error()))
					}
				}
				return nil
			} else {
				return func() bool {
					if _, ok := pending.LoadAndDelete(reqMessage.Seq); ok {
						ctx.stopTimer()
						return true
					} else {
						return false
					}
				}
			}
		}
	}
}

func (c *Client) Call(ctx context.Context, channel Channel, method string, arg interface{}, ret interface{}) error {
	if b, err := c.codec.Encode(arg); err != nil {
		logger.Panicf("encode error:%v", err)
		return nil
	} else {
		reqMessage := &RequestMsg{
			Seq:    atomic.AddUint64(&c.nextSequence, 1),
			Method: method,
			Arg:    b,
		}
		if ret != nil {
			waitC := make(chan error, 1)
			pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]

			pending.Store(reqMessage.Seq, &callContext{
				respReceiver: ret,
				onResponse: func(_ interface{}, err error) {
					waitC <- err
				},
			})

			if err = channel.SendRequestWithContext(ctx, reqMessage); err != nil {
				pending.Delete(reqMessage.Seq)
				if e, ok := err.(net.Error); ok && e.Timeout() {
					return newError(ErrTimeout, "timeout")
				} else {
					return newError(ErrSend, err.Error())
				}
			}

			select {
			case err := <-waitC:
				return err
			case <-ctx.Done():
				pending.Delete(reqMessage.Seq)
				switch ctx.Err() {
				case context.Canceled:
					return newError(ErrCancel, "canceled")
				case context.DeadlineExceeded:
					return newError(ErrTimeout, "timeout")
				default:
					return newError(ErrOther, "unknow")
				}
			}
		} else {
			reqMessage.Oneway = true
			if err = channel.SendRequestWithContext(ctx, reqMessage); nil != err {
				if e, ok := err.(net.Error); ok && e.Timeout() {
					return newError(ErrTimeout, "timeout")
				} else {
					return newError(ErrSend, err.Error())
				}
			} else {
				return nil
			}
		}
	}
}

type callContext struct {
	onResponse    RespCB
	deadlineTimer atomic.Value
	fired         int32
	respReceiver  interface{}
}


	nextSequence uint64
	codec        Codec
	pendingCall  [32]sync.Map



*/

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
        //public int fired = 0;

        public Mutex mtx = new Mutex();

        private Rpc.Proto.rpcResponse? response = null;

        private SemaphoreSlim semaphore = new SemaphoreSlim(1);

        public async Task<Rpc.Proto.rpcResponse?>  Wait(CancellationToken cancellationToken)
        {
            Rpc.Proto.rpcResponse? resp;
            await semaphore.WaitAsync(cancellationToken);
            mtx.WaitOne();
            resp = response;
            mtx.ReleaseMutex();
            return resp;
        }   

        public void Wakeup(Rpc.Proto.rpcResponse response)
        {
            //if(Interlocked.CompareExchange(ref fired,1,0) == 0)
            //{
            mtx.WaitOne();
            this.response = response;
            mtx.ReleaseMutex();
            semaphore.Release();
            //}
        } 
    }
    
    private ulong nextSeqno = 0;

    private Mutex mtx = new Mutex();

    private Dictionary<ulong,callContext> pendingCall = new Dictionary<ulong,callContext>();

    public void Call<Arg>(RpcChannelI channel,string method,Arg arg,CancellationToken cancellationToken) where Arg : IMessage<Arg>
    {
        Rpc.Proto.rpcRequest request = new Rpc.Proto.rpcRequest();
        request.Seq = Interlocked.Add(ref nextSeqno,1);
        request.Method = method;
        request.Arg = ByteString.CopyFrom(RpcCodec.Codec.Encode(arg));
        request.Oneway = true;
        channel.SendRequest(request);
    }

    public async Task<Response<Ret>> Call<Arg,Ret>(RpcChannelI channel,string method,Arg arg,CancellationToken cancellationToken) where Arg : IMessage<Arg> where Ret : IMessage<Ret>,new()
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

        Rpc.Proto.rpcResponse? resp = null;
        try{
            channel.SendRequest(request);
            resp = await context.Wait(cancellationToken);
        }
        catch(Exception ee)
        {
            if(!cancellationToken.IsCancellationRequested){
                e = ee;
            }
        }

        if(resp is null) {
            mtx.WaitOne();
            pendingCall.Remove(request.Seq);
            mtx.ReleaseMutex();
            if(!(e is null)){
                throw e;
            } else {
                //无法区分cancel原因，统一按超时处理
                return new Response<Ret>(new RpcError(RpcError.ErrTimeout,"timeout"));
            }
        } else {
            if(resp.ErrCode == 0) {
                Ret ret = new Ret();
                RpcCodec.Codec.Decode(resp.Ret.ToByteArray(),ret);
                return new Response<Ret>(ret);
            } else {
                return new Response<Ret>(new RpcError(resp.ErrCode,resp.ErrDesc));
            }
        }
    }

    public void OnMessage(Rpc.Proto.rpcResponse respMsg)
    {
        mtx.WaitOne();
        callContext? ctx = pendingCall[respMsg.Seq];
        if(!(ctx is null)){
            pendingCall.Remove(respMsg.Seq);
            mtx.ReleaseMutex();
            ctx.Wakeup(respMsg);
        } else {
            mtx.ReleaseMutex();
        }
    }
}

//public class RpcServer
//{

//}
