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

public interface PacketReceiverI
{
    Task<Object> Recv(NetworkStream stream,CancellationToken token);
}

public class Socket
{
    private TcpClient socket;
    private BufferBlock<MessageI?> sendList = new BufferBlock<MessageI?>();

    private Task? sendTask;

    private Task? recvTask;

    private CancellationTokenSource calcelRead = new CancellationTokenSource();

    private bool closed = false;

    private  Mutex mtx = new Mutex();

    private Action<Socket>? closeCallback;

    Socket(TcpClient c)
    {
        socket = c;
    }

    ~Socket()
    {
        socket?.Close();
        sendTask?.Dispose();
        recvTask?.Dispose();
    }

    public Socket SetCloseCallback(Action<Socket> closeCallback) {
        mtx.WaitOne();
        this.closeCallback = closeCallback;
        mtx.ReleaseMutex();
        return this;
    }

    public Socket Start(PacketReceiverI receiver,Func<Socket, Object,bool> packetCallback) 
    {
        mtx.WaitOne();
        if(sendTask == null && recvTask == null)
        {
            sendThread();
            recvThread(receiver,packetCallback);
        }
        mtx.ReleaseMutex();
        return this;
    }

    public async void Send(MessageI msg) 
    {
        if(msg != null)
        {
            await sendList.SendAsync(msg);
        }
    }

    public void Close()
    {
        Action<Socket>? closeCallback = null;
        var flagWait = false;
        mtx.WaitOne();
        if(closed){
            mtx.ReleaseMutex();
            return;
        }

        closed = true;
        calcelRead.Cancel();
        sendList.Post(null);
        if(!(sendTask == null && recvTask == null))
        {
            flagWait = true;
        }
        closeCallback = this.closeCallback;
        mtx.ReleaseMutex();        

        if(flagWait)
        {
            sendTask?.Wait();
            recvTask?.Wait();
        }

        if(closeCallback != null)
        {
            closeCallback(this);
        }
    }

    private void sendThread()
    {
        sendTask = Task.Run(async () =>
        {
            try
            {
                var writer = socket.GetStream();
                while (true)
                {
                    var msg = await sendList.ReceiveAsync();
                    if(msg != null)
                    {
                        var data = msg.Encode();
                        if(data != null)
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
            finally
            {
               Close();
            }
        });
    }

    private void recvThread(PacketReceiverI receiver,Func<Socket, Object,bool> packetCallback)
    {
        recvTask = Task.Run(async () =>
        {
            try
            {
                var stream = socket.GetStream();
                var cancelToken = calcelRead.Token;
                while (true)
                {
                    var packet = await receiver.Recv(stream,cancelToken);
                    if(cancelToken.IsCancellationRequested)
                    {
                        break;
                    } else {
                        if(!packetCallback(this,packet))
                        {
                            break;
                        }
                    }
                }
            }
            finally
            {
                Close();
            }
        });
    }
}