using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
namespace ClusterDotNet;

//interface of server <-> server message 
public interface ISSMsg 
{
    void Encode(MemoryStream stream);
}

public interface IPacketReceiver
{   
    Task<Object?> Recv(Func<byte[], int, int,Task<int>> recvfunc);
}

public class Session
{
    private NetworkStream stream;

    private BufferBlock<ISSMsg?> sendList = new BufferBlock<ISSMsg?>();

    private int closed = 0;

    private int started = 0;

    private Action<Session>? closeCallback = null;

    private Action<Session>? onRecvTimeout = null;

    private int recvTimeout;

    private int sendTimeout;

    private int threadCount = 0;

    public Session(Socket s)
    {
        stream = new NetworkStream(s,ownsSocket: true);
    }

    ~Session()
    {
        stream.Dispose();
    }

    public Session SetRecvTimeout(int recvTimeout,Action<Session>? onRecvTimeout)
    {
        Interlocked.Exchange(ref this.onRecvTimeout,onRecvTimeout);
        this.recvTimeout = recvTimeout;
        return this;
    }


    public Session SetSendTimeout(int sendTimeout)
    {
        this.sendTimeout = sendTimeout;
        return this;
    }

    public Session SetCloseCallback(Action<Session> closeCallback) {
        Interlocked.Exchange(ref this.closeCallback,closeCallback);
        return this;
    }



    public Session Start(IPacketReceiver receiver,Func<Session, Object,bool> packetCallback) 
    {
        if(0 == Interlocked.CompareExchange(ref started,1,0)){
            threadCount = 2;
            sendThread();
            recvThread(receiver,packetCallback);
        }
        return this;
    }

    public async void Send(ISSMsg msg) 
    {   
        if(closed != 0) {
            return;
        }
        if(!(msg is null))
        {
            await sendList.SendAsync(msg);
        }
    }

    public void Close() 
    {
        if(0 == Interlocked.CompareExchange(ref closed,1,0)){
            sendList.Post(null);
            stream.Socket.Shutdown(SocketShutdown.Receive);
            if(started == 0)
            {
                stream.Dispose();
                var closeCallback = Interlocked.Exchange(ref this.closeCallback,this.closeCallback);
                if(!(closeCallback is null)){
                    Task.Run(() =>
                    {
                        closeCallback(this);
                    });
                }
            }
        }
    }

    private void sendThread()
    {
        Task.Run(async () =>
        {
            const int maxSendSize = 65535;
            using MemoryStream memoryStream = new MemoryStream();
            bool finish = false;
            CancellationTokenSource token = new CancellationTokenSource();
            for(;!finish;){
                try{
                    var msg = await sendList.ReceiveAsync();
                    if(msg is null) {
                        break;
                    } else {
                        msg.Encode(memoryStream);
                        for (;memoryStream.Length < maxSendSize;){
                            if(sendList.TryReceive(out msg)){
                                if(msg is null){
                                    finish = true;
                                    break;
                                } else {
                                    msg.Encode(memoryStream);
                                }
                            } else {
                                break;
                            }
                        }
                        if(memoryStream.Length > 0) {
                            var data = memoryStream.GetBuffer();
                            if(sendTimeout > 0) {
                                if(!token.TryReset())
                                {
                                    token.Dispose();
                                    token = new CancellationTokenSource();
                                }
                                token.CancelAfter(sendTimeout);
                                await stream.WriteAsync(data, 0, (int)memoryStream.Position,token.Token);
                            } else {
                                await stream.WriteAsync(data, 0, (int)memoryStream.Position);
                            }    
                            memoryStream.Position = 0;
                            memoryStream.SetLength(0);
                        }
                    }
                }
                catch(OperationCanceledException)
                {
                    break;
                }
                catch(Exception e)
                {
                    Console.WriteLine(e);
                    break;
                }
            }

            token.Dispose();
            memoryStream.Dispose();

            if(0 == Interlocked.CompareExchange(ref closed,1,0)){
                stream.Socket.Shutdown(SocketShutdown.Receive);   
            }

            if(Interlocked.Add(ref threadCount,-1) == 0) {
                stream.Dispose();
                var closeCallback = Interlocked.Exchange(ref this.closeCallback,this.closeCallback);
                if(!(closeCallback is null)){
                    closeCallback(this);
                }               
            }
        });
    }

    private void recvThread(IPacketReceiver receiver,Func<Session, Object,bool> packetCallback)
    {
        Task.Run(async () =>
        {
            CancellationTokenSource token = new CancellationTokenSource();

            Func<byte[], int, int,Task<int>> recvfunc = (byte[] buff, int offset, int count) =>{
                if(recvTimeout > 0) {
                    if(!token.TryReset())
                    {
                        token.Dispose();
                        token = new CancellationTokenSource();
                    }
                    token.CancelAfter(recvTimeout);
                    return stream.ReadAsync(buff, offset, count,token.Token);
                } else {
                    return stream.ReadAsync(buff, offset, count);
                }
            };       


            for(;closed==0;) {
                try{
                    var packet = await receiver.Recv(recvfunc);
                    if(packet is null || !packetCallback(this,packet))
                    {
                        break;
                    }                   
                }
                catch(OperationCanceledException)
                {
                    var onRecvTimeout = Interlocked.Exchange(ref this.onRecvTimeout,this.onRecvTimeout);
                    if(!(onRecvTimeout is null)){
                        onRecvTimeout(this);
                    } else {
                        break;
                    }                           
                }
                catch(Exception e)
                {
                    Console.WriteLine(e);
                    break;
                }
            }
            
            token.Dispose();
            if(0 == Interlocked.CompareExchange(ref closed,1,0)){
                sendList.Post(null);
            }

            if(Interlocked.Add(ref threadCount,-1) == 0) {
                stream.Dispose();
                var closeCallback = Interlocked.Exchange(ref this.closeCallback,this.closeCallback);
                if(!(closeCallback is null)){
                    closeCallback(this);
                }                
            }
        });
    }
}