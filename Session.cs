using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
namespace SanguoDotNet;

public interface MessageI 
{
    void Encode(MemoryStream stream);
}

public interface StreamReaderI
{
    Task<int> Recv(byte[] buffer, int offset, int count);
}

public interface PacketReceiverI
{
    Task<Object?> Recv(StreamReaderI reader);
}

public class Session
{
    private class StreamReader : StreamReaderI
    {
        public  int Timeout = 0;

        private CancellationTokenSource token = new CancellationTokenSource();

        private NetworkStream stream;

        public StreamReader(NetworkStream stream)
        {
            this.stream = stream;
        }

        public Task<int> Recv(byte[] buff, int offset, int count) 
        {
            if(Timeout > 0) {
                if(!token.TryReset())
                {
                    token = new CancellationTokenSource();
                }
                token.CancelAfter(Timeout);
                return stream.ReadAsync(buff, offset, count,token.Token);
            } else {
                return stream.ReadAsync(buff, offset, count);
            }
        }
    }

    private NetworkStream stream;

    private Socket socket;

    private BufferBlock<MessageI?> sendList = new BufferBlock<MessageI?>();

    private int closed = 0;

    private int started = 0;

    private Action<Session>? closeCallback = null;

    private Action<Session>? onRecvTimeout = null;

    private int recvTimeout;

    private int sendTimeout;

    private int threadCount = 0;

    public Session(Socket s)
    {
        socket = s;
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



    public Session Start(PacketReceiverI receiver,Func<Session, Object,bool> packetCallback) 
    {
        if(0 == Interlocked.CompareExchange(ref started,1,0)){
            threadCount = 2;
            sendThread();
            recvThread(receiver,packetCallback);
        }
        return this;
    }

    public async void Send(MessageI msg) 
    {   
        if(!(msg is null))
        {
            await sendList.SendAsync(msg);
        }
    }

    public void Close() 
    {
        if(0 == Interlocked.CompareExchange(ref closed,1,0)){
            sendList.Post(null);
            socket.Shutdown(SocketShutdown.Receive);
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
            MemoryStream memoryStream = new MemoryStream();
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
                            var data = memoryStream.ToArray();
                            if(sendTimeout > 0) {
                                if(!token.TryReset())
                                {
                                    token = new CancellationTokenSource();
                                }
                                token.CancelAfter(sendTimeout);
                                await stream.WriteAsync(data, 0, data.Length,token.Token);
                            } else {
                                await stream.WriteAsync(data, 0, data.Length);
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

            memoryStream.Dispose();

            if(0 == Interlocked.CompareExchange(ref closed,1,0)){
                socket.Shutdown(SocketShutdown.Receive);   
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

    private void recvThread(PacketReceiverI receiver,Func<Session, Object,bool> packetCallback)
    {
        Task.Run(async () =>
        {
            var reader = new StreamReader(stream);
            for(;closed==0;) {
                reader.Timeout = recvTimeout;
                try{
                    var packet = await receiver.Recv(reader);
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