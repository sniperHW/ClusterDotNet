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

    private class Cancellation
    {
        private Mutex mtx = new Mutex();

        private CancellationTokenSource token = new CancellationTokenSource(); 
        
        public  int Timeout = 0;

        private bool canceled = false;

        public void Cancel()
        {
            mtx.WaitOne();
            canceled = true;
            token.Cancel();
            mtx.ReleaseMutex();
        }

        public System.Threading.CancellationToken ResetToken()
        {

            System.Threading.CancellationToken tk;

            mtx.WaitOne();

            if(!canceled){
                if(!token.TryReset())
                {
                    token = new CancellationTokenSource();
                }
                if(Timeout>0)
                {
                    token.CancelAfter(Timeout);
                }
            }

            tk = token.Token;

            mtx.ReleaseMutex();

            return tk;
        }
    }

    private Socket socket;

    private BufferBlock<MessageI?> sendList = new BufferBlock<MessageI?>();

    private Cancellation cancellation = new Cancellation();

    private int closed = 0;

    private int started = 0;

    private  Mutex mtx = new Mutex();

    private Action<Session>? closeCallback;

    private Action<Session>? onRecvTimeout;

    private int threadCount = 0;

    public Session(Socket s)
    {
        socket = s;
    }

    ~Session()
    {
        socket.Close();
    }

    public Session SetRecvTimeout(int recvTimeout,Action<Session>? onRecvTimeout)
    {
        mtx.WaitOne();
        this.onRecvTimeout = onRecvTimeout;
        mtx.ReleaseMutex();
        cancellation.Timeout = recvTimeout;
        return this;
    }

    public Session SetCloseCallback(Action<Session> closeCallback) {
        mtx.WaitOne();
        this.closeCallback = closeCallback;
        mtx.ReleaseMutex();
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
            Action<Session>? closeCallback = null;
            sendList.Post(null);
            cancellation.Cancel();
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
        Task.Run(async () =>
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
                    cancellation.Cancel();    
                }


                if(Interlocked.Add(ref threadCount,-1) == 0) {
                    Action<Session>? closeCallback = null;
                    mtx.WaitOne();
                    closeCallback = this.closeCallback;
                    mtx.ReleaseMutex();
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
        Task.Run(async () =>
        {
            using NetworkStream stream = new(socket, ownsSocket: true);
            var reader = new StreamReader(stream);
            for(;closed==0;) {
                reader.Token = cancellation.ResetToken();
                try{
                    var packet = await receiver.Recv(reader);
                    if(packet is null)
                    {
                        break;
                    } else if(!packetCallback(this,packet)){
                        break;
                    }                    
                }
                catch(OperationCanceledException)
                {
                    if(closed==1) {
                        break;
                    } else {
                        Action<Session>? onRecvTimeout;
                        mtx.WaitOne();
                        onRecvTimeout = this.onRecvTimeout;
                        mtx.ReleaseMutex();
                        if(!(onRecvTimeout is null)){
                            onRecvTimeout(this);
                        }
                    }                    
                }
                catch(Exception e)
                {
                    Console.WriteLine(e);
                }
            }

            if(0 == Interlocked.CompareExchange(ref closed,1,0)){
                sendList.Post(null);
            }

            if(Interlocked.Add(ref threadCount,-1) == 0) {
                Action<Session>? closeCallback = null;
                mtx.WaitOne();
                closeCallback = this.closeCallback;
                mtx.ReleaseMutex();
                socket.Close();
                if(!(closeCallback is null))
                {
                    closeCallback(this);
                }                
            }
        });
    }
}