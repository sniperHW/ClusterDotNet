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

public interface ReceiveAbleI
{
    Task<int> Recv(byte[] buffer, int offset, int count);
}

public interface PacketReceiverI
{
    Task<Object?> Recv(ReceiveAbleI receiver);
}

public class Session
{

    private class StreamReceiver : ReceiveAbleI
    {
        private CancellationToken token;
        private NetworkStream stream;

        public StreamReceiver(NetworkStream stream,CancellationToken token)
        {
            this.stream = stream;
            this.token = token;
        }

        public Task<int> Recv(byte[] buff, int offset, int count) 
        {
            return stream.ReadAsync(buff, offset, count, token);
        }
    }

    private Socket socket;
    private BufferBlock<MessageI?> sendList = new BufferBlock<MessageI?>();

    private Task? sendTask;

    private Task? recvTask;

    private CancellationTokenSource calcelRead = new CancellationTokenSource();

    private Int32 closed = 0;

    private  Mutex mtx = new Mutex();

    private Action<Session>? closeCallback;

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

    public Session SetCloseCallback(Action<Session> closeCallback) {
        mtx.WaitOne();
        this.closeCallback = closeCallback;
        mtx.ReleaseMutex();
        return this;
    }

    public Session Start(PacketReceiverI receiver,Func<Session, Object,bool> packetCallback) 
    {
        mtx.WaitOne();
        if(sendTask is null && recvTask is null)
        {
            sendThread();
            recvThread(receiver,packetCallback);
        }
        mtx.ReleaseMutex();
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
            Task? sendTask;
            Task? recvTask;
            mtx.WaitOne();
            calcelRead.Cancel();
            sendList.Post(null);
            closeCallback = this.closeCallback;
            sendTask = this.sendTask;
            recvTask = this.recvTask;
            mtx.ReleaseMutex();
            sendTask?.Wait();
            recvTask?.Wait();
            socket.Close();
            if(!(closeCallback is null))
            {
                closeCallback(this);
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
                    mtx.WaitOne();
                    calcelRead.Cancel();
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
            var cancelToken = calcelRead.Token;
            try
            {
                using NetworkStream reader = new(socket, ownsSocket: true);
                StreamReceiver streamReceiver = new StreamReceiver(reader,cancelToken); 
                for(;;)
                {
                    var packet = await receiver.Recv(streamReceiver);
                    if(packet is null)
                    {
                        break;
                    } else if(!packetCallback(this,packet)){
                        break;
                    }
                    
                }
            }
            catch(Exception e)
            {
                if(!cancelToken.IsCancellationRequested)
                {
                    Console.WriteLine(e);
                }
            }
            finally
            {   
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
            }
        });
    }
}