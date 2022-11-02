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
    Task<Object?> Recv(System.Net.Sockets.NetworkStream stream,CancellationToken token);
}

public class Session
{
    private Socket socket;
    private BufferBlock<MessageI?> sendList = new BufferBlock<MessageI?>();

    private Task? sendTask;

    private Task? recvTask;

    private CancellationTokenSource calcelRead = new CancellationTokenSource();

    private bool closed = false;

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
        Action<Session>? closeCallback = null;
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
                using NetworkStream writer = new(socket, ownsSocket: true);
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
            catch(Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
               Close();
            }
        });
    }

    private void recvThread(PacketReceiverI receiver,Func<Session, Object,bool> packetCallback)
    {
        recvTask = Task.Run(async () =>
        {
            try
            {
                using NetworkStream reader = new(socket, ownsSocket: true);
                var cancelToken = calcelRead.Token;
                while (true)
                {
                    var packet = await receiver.Recv(reader,cancelToken);
                    if(packet == null)
                    {
                        break;
                    } else {
                        bool ok = packetCallback(this,packet);
                        Console.WriteLine(ok); 
                        if(!ok)
                        {
                            Console.WriteLine("break");
                            break;
                        }
                    }
                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {   
                Close();
            }
        });
    }
}