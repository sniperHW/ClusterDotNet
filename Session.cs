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
    byte[] Encode();
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
        public CancellationToken Token;
        private NetworkStream stream;

        public StreamReader(NetworkStream stream)
        {
            this.stream = stream;
        }

        public Task<int> Recv(byte[] buff, int offset, int count) 
        {
            return stream.ReadAsync(buff, offset, count, Token);
        }
    }

    private Socket socket;
    private BufferBlock<MessageI?> sendList = new BufferBlock<MessageI?>();

    private Task? sendTask;

    private Task? recvTask;


    private Mutex mtxCancellation = new Mutex();

    private CancellationTokenSource cancelToken = new CancellationTokenSource();

    private int closed = 0;

    private int started = 0;

    private  Mutex mtx = new Mutex();

    private Action<Session>? closeCallback;

    private Func<bool>? onRecvTimeout;

    private int recvTimeout = 0;

    public Session(Socket s)
    {
        socket = s;
    }

    ~Session()
    {
        socket?.Close();
        sendTask?.Dispose();
        recvTask?.Dispose();
    }

    public Session SetRecvTimeout(int recvTimeout,Func<bool>? onRecvTimeout)
    {
        mtx.WaitOne();
        this.recvTimeout = recvTimeout;
        this.onRecvTimeout = onRecvTimeout;
        mtx.ReleaseMutex();
        return this;
    }

    public Session SetCloseCallback(Action<Session> closeCallback) {
        mtx.WaitOne();
        this.closeCallback = closeCallback;
        mtx.ReleaseMutex();
        return this;
    }

    private void callCancel()
    {
        mtx.WaitOne();
        cancelToken.Cancel();
        mtx.ReleaseMutex();
    }

    private System.Threading.CancellationToken resetCancelToken()
    {

        System.Threading.CancellationToken token;

        mtx.WaitOne();

        if(closed == 0){
            if(!cancelToken.TryReset())
            {
                cancelToken = new CancellationTokenSource();
            }
            if(recvTimeout>0)
            {
                cancelToken.CancelAfter(recvTimeout);
            }
        }

        token = cancelToken.Token;

        mtx.ReleaseMutex();

        return token;
    }

    public Session Start(PacketReceiverI receiver,Func<Session, Object,bool> packetCallback) 
    {
        if(0 == Interlocked.CompareExchange(ref started,1,0)){
            mtx.WaitOne();
            if(sendTask is null && recvTask is null)
            {
                sendThread();
                recvThread(receiver,packetCallback);
            }
            mtx.ReleaseMutex();
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
            Action<Session>? closeCallback = null;
            sendList.Post(null);
            callCancel();
            mtx.WaitOne();
            closeCallback = this.closeCallback;
            mtx.ReleaseMutex();
            if(started == 0)
            {
                socket.Close();
                if(!(closeCallback is null))
                {
                    closeCallback(this);
                }
            }
        }
    }

    private void sendThread()
    {
        sendTask = Task.Run(async () =>
        {
            try
            {
                using NetworkStream writer = new(socket, ownsSocket: true);
                while (true)
                {
                    var msg = await sendList.ReceiveAsync();
                    if(!(msg is null))
                    {
                        var data = msg.Encode();
                        if(!(data is null))
                        {
                            await writer.WriteAsync(data, 0, data.Length);
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            } 
            catch(Exception e) 
            {
                Console.WriteLine(e);   
            } 
            finally 
            {
                if(0 == Interlocked.CompareExchange(ref closed,1,0)){
                    Action<Session>? closeCallback = null;
                    Task? recvTask;
                    callCancel();
                    mtx.WaitOne();
                    closeCallback = this.closeCallback;
                    recvTask = this.recvTask; 
                    mtx.ReleaseMutex();
                    recvTask?.Wait();
                    socket.Close();
                    if(!(closeCallback is null))
                    {
                        closeCallback(this);
                    }     
                }
            }
        });
    }

    private void recvThread(PacketReceiverI receiver,Func<Session, Object,bool> packetCallback)
    {
        recvTask = Task.Run(async () =>
        {
            using NetworkStream stream = new(socket, ownsSocket: true);
            var reader = new StreamReader(stream);
            for(;;) {
                reader.Token = resetCancelToken();
                try{
                    var packet = await receiver.Recv(reader);
                    if(packet is null)
                    {
                        break;
                    } else if(!packetCallback(this,packet)){
                        break;
                    }                    
                }
                catch(Exception e)
                {
                    if(!reader.Token.IsCancellationRequested)
                    {
                        Console.WriteLine(e);
                        break;
                    } else {
                        if(closed==1) {
                            break;
                        } else {
                            Func<bool>? onRecvTimeout;
                            mtx.WaitOne();
                            onRecvTimeout = this.onRecvTimeout;
                            mtx.ReleaseMutex();
                            if(onRecvTimeout == null || !onRecvTimeout()){
                                break;
                            }
                        }
                    }
                }
            }

            if(0 == Interlocked.CompareExchange(ref closed,1,0)){
                Action<Session>? closeCallback = null;
                Task? sendTask;
                sendList.Post(null);
                mtx.WaitOne();
                closeCallback = this.closeCallback;
                sendTask = this.sendTask;
                mtx.ReleaseMutex();
                sendTask?.Wait();
                socket.Close();
                if(!(closeCallback is null))
                {
                    closeCallback(this);
                }
            }
        });
    }
}