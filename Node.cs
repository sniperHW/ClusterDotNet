using System;
//using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
namespace SanguoDotNet;

public class DiscoveryNode : IComparable
{
    public Addr Addr{get;}
    public bool Export{get;}
    public bool Available{get;set;}


    public int CompareTo(Object? obj) 
    {
        if(obj == null) return 1;
        
        var other = obj as DiscoveryNode;

        if(other is null) {
            throw new ArgumentException("Object is not a DiscoveryNode");
        } else {
            return Addr.LogicAddr.CompareTo(other.Addr.LogicAddr);
        }   
    }


    public DiscoveryNode(Addr addr,bool export,bool available)
    {
        Addr = addr;
        Export = export;
        Available = available;
    }

}

public interface IDiscovery
{
    void Subscribe(Action<DiscoveryNode[]> onUpdate);
}


internal class NodeCache
{
    private Mutex mtx = new Mutex();
    private LogicAddr localAddr;
    private int initOK = 0;
    private SemaphoreSlim semaphore = new SemaphoreSlim(0);
    private Random rnd = new Random();
    Dictionary<uint,Node> nodes = new Dictionary<uint,Node>();

    Dictionary<uint,ArrayList> nodeByType = new Dictionary<uint,ArrayList>();

    Dictionary<uint,ArrayList> harborsByCluster = new Dictionary<uint,ArrayList>();

    public NodeCache(LogicAddr localAddr) 
    {
        this.localAddr = localAddr;
    }

    public void WaitInit(CancellationToken cancellationToken) {
        semaphore.Wait(cancellationToken);
    }

    public void AddNodeByType(Node n)
    {
        if(this.nodeByType.ContainsKey(n.Addr.LogicAddr.Type())) {
            ArrayList nodeByType = this.nodeByType[n.Addr.LogicAddr.Type()];
            nodeByType.Add(n);
        } else {
            ArrayList nodeByType = new ArrayList();
            nodeByType.Add(n);
            this.nodeByType[n.Addr.LogicAddr.Type()] = nodeByType;
        }
    }

    public void RemoveNodeByType(Node n)
    {
        if(this.nodeByType.ContainsKey(n.Addr.LogicAddr.Type())) {
            ArrayList nodeByType = this.nodeByType[n.Addr.LogicAddr.Type()];
            for(var i = 0;i < nodeByType.Count;i++){
                var nn = (Node?)nodeByType[i];
                if(!(nn is null) && nn.Addr.LogicAddr == n.Addr.LogicAddr){
                    nodeByType.RemoveAt(i);
                    break;
                }                
            }
        }
    }

    public void AddHarborsByCluster(Node n)
    {
        if(this.harborsByCluster.ContainsKey(n.Addr.LogicAddr.Cluster())) {
            ArrayList harborsByCluster = this.harborsByCluster[n.Addr.LogicAddr.Cluster()];
            harborsByCluster.Add(n);
        } else {
            ArrayList harborsByCluster = new ArrayList();
            harborsByCluster.Add(n);
            this.harborsByCluster[n.Addr.LogicAddr.Cluster()] = harborsByCluster;
        }
    }

    public void RemoveHarborsByCluster(Node n)
    {
        if(this.harborsByCluster.ContainsKey(n.Addr.LogicAddr.Cluster())) {
            ArrayList harborsByCluster = this.harborsByCluster[n.Addr.LogicAddr.Cluster()];
            for(var i = 0;i < harborsByCluster.Count;i++){
                var nn = (Node?)harborsByCluster[i];
                if(!(nn is null) && nn.Addr.LogicAddr == n.Addr.LogicAddr){
                    harborsByCluster.RemoveAt(i);
                    break;
                }                
            }
        }
    }

    public Node? GetHarborByCluster(uint cluster,LogicAddr m)
    {
        mtx.WaitOne();
        if(!harborsByCluster.ContainsKey(cluster)){
            mtx.ReleaseMutex();
            return null;
        } else {
            ArrayList harbors = harborsByCluster[cluster];
            var n = (Node?)harbors[rnd.Next(0,harbors.Count)];
            mtx.ReleaseMutex();
            return n;
        }
    }

    public Node? GetNodeByType(uint tt,int num)
    {

        mtx.WaitOne();
        if(!nodeByType.ContainsKey(tt)) {
            mtx.ReleaseMutex();
            return null;
        } else {
            ArrayList nodes = nodeByType[tt];
            Node? n = null;
            if(num == 0) {
                n = (Node?)nodes[rnd.Next(0,nodes.Count)];
            } else {
                n = (Node?)nodes[num%nodes.Count];
            }
            
            mtx.ReleaseMutex();
            return n;            
        }       
    }

    public Node? GetNodeByLogicAddr(LogicAddr logicAddr)
    {
        Node? n = null;
        mtx.WaitOne();
        if(nodes.ContainsKey(logicAddr.ToUint32())){
            n = nodes[logicAddr.ToUint32()];
        }
        mtx.ReleaseMutex();
        return n;        
    }

    public void Stop()
    {
        mtx.WaitOne();
        foreach( KeyValuePair<uint,Node> kvp in nodes ){
            kvp.Value.closeSession();
        }
        mtx.ReleaseMutex();
    }

    public void onNodeUpdate(Sanguo sanguo, DiscoveryNode []nodeinfo) {
        List<DiscoveryNode> interested = new List<DiscoveryNode>();
        for(var k = 0;k < nodeinfo.Length;k++){
            DiscoveryNode v = nodeinfo[k];
            if(v.Export || v.Addr.LogicAddr.Cluster() == localAddr.Cluster()){
                interested.Add(v);
            } else if(localAddr.Type() == LogicAddr.HarbarType && v.Addr.LogicAddr.Type() == LogicAddr.HarbarType){
                interested.Add(v);
            }
        }

        DiscoveryNode[] nodesFromDiscovery = interested.ToArray();
        Array.Sort(nodesFromDiscovery);


        mtx.WaitOne();

        Node[] localNodes = new Node[nodes.Count];
        var i = 0;
        var j = 0;
        foreach( KeyValuePair<uint,Node> kvp in nodes ){
            localNodes[i++] = kvp.Value;
        }
        Array.Sort(localNodes);
        i = 0;
        var removeSelf = false;
        for(; i < nodesFromDiscovery.Length && j < localNodes.Length ;)
        {

            var localNode = localNodes[j];
            var updateNode = (DiscoveryNode)nodesFromDiscovery[i];
            if(updateNode.Addr.LogicAddr == localNode.Addr.LogicAddr){
                if(updateNode.Addr.NetAddr != localNode.Addr.NetAddr){
                    //网络地址发生变更
                    if(localNode.Addr.LogicAddr == localAddr){
                        removeSelf = true;
                        break;
                    } else {
                        localNode.closeSession();
                        localNode.Addr.NetAddr = updateNode.Addr.NetAddr;
                    }
                }

                if(updateNode.Available){
                    if(!localNode.Available){
                        localNode.Available = true;
                        if(localNode.Addr.LogicAddr.Type() != LogicAddr.HarbarType){
                            AddNodeByType(localNode);
                        } else {
                            AddHarborsByCluster(localNode);
                        }
                    }
                } else {
                    if(localNode.Available){
                        localNode.Available = false;
                        if(localNode.Addr.LogicAddr.Type() != LogicAddr.HarbarType){
                            RemoveNodeByType(localNode);
                        } else {
                            RemoveHarborsByCluster(localNode);
                        }
                    }
                }
                j++;
                i++;
            } else if(updateNode.Addr.LogicAddr.ToUint32() > localNode.Addr.LogicAddr.ToUint32()){
                //local  1 2 3 4 5 6
			    //update 1 2 4 5 6
			    //移除节点
                if(localNode.Addr.LogicAddr == localAddr){
                    removeSelf = true;
                    break;
                } else {
                    nodes.Remove(localNode.Addr.LogicAddr.ToUint32());
                    if(localNode.Available){
                        if(localNode.Addr.LogicAddr.Type() != LogicAddr.HarbarType){
                            RemoveNodeByType(localNode);
                        } else {
                            RemoveHarborsByCluster(localNode);
                        }
                    }
                    localNode.closeSession();
                }
                j++;
            } else {
			    //local  1 2 4 5 6
			    //update 1 2 3 4 5 6
			    //添加节点
                Node n = new Node(updateNode.Addr,updateNode.Available,updateNode.Export);
                nodes[updateNode.Addr.LogicAddr.ToUint32()] = n;

                if(n.Available){
                    if(n.Addr.LogicAddr.Type() != LogicAddr.HarbarType){
                        AddNodeByType(n);
                    } else {
                        AddHarborsByCluster(n);
                    }
                }
                i++;    
            }
        }

        if(!removeSelf) {
            for(; i < nodesFromDiscovery.Length; i++) {
                var updateNode = (DiscoveryNode)nodesFromDiscovery[i];
                Node n = new Node(updateNode.Addr,updateNode.Available,updateNode.Export);
                nodes[updateNode.Addr.LogicAddr.ToUint32()] = n;

                if(n.Available){
                    if(n.Addr.LogicAddr.Type() != LogicAddr.HarbarType){
                        AddNodeByType(n);
                    } else {
                        AddHarborsByCluster(n);
                    }
                }
            }

            for(;j < localNodes.Length; j++){
                var localNode = localNodes[j];
                if(localNode.Addr.LogicAddr == localAddr){
                    removeSelf = true;
                    break;
                } else {
                    nodes.Remove(localNode.Addr.LogicAddr.ToUint32());
                    if(localNode.Available){
                        if(localNode.Addr.LogicAddr.Type() != LogicAddr.HarbarType){
                            RemoveNodeByType(localNode);
                        } else {
                            RemoveHarborsByCluster(localNode);
                        }
                    }
                    localNode.closeSession();                
                }
            }
        }

        mtx.ReleaseMutex();

        if(removeSelf) {
            Task.Run(()=>sanguo.Stop());

        } else {
            if(Interlocked.CompareExchange(ref initOK,1,0) == 0){
                semaphore.Release();
            }            
        }
    }    
}

internal class Node : DiscoveryNode
{

    private class pendingMessage
    {
        public ISSMsg message;
        public DateTime? deadline;
        public CancellationToken? cancellationToken;

        public pendingMessage(ISSMsg message,DateTime? deadline,CancellationToken? cancellationToken)
        {
            this.message = message;
            this.deadline = deadline;
            this.cancellationToken = cancellationToken;
        }
    }

    private class StreamClient
    {

        public class OpenRequest
        {
            public Smux.Stream?  stream;
            public Exception?    exception;
            private SemaphoreSlim semaphore = new SemaphoreSlim(0);
            public async Task WaitAsync(CancellationToken cancellationToken)
            {
                await semaphore.WaitAsync(cancellationToken);
            }
            public void OnOpenResponse(Smux.Stream? stream,Exception? exception)
            {
                Interlocked.Exchange(ref this.stream,stream);
                Interlocked.Exchange(ref this.exception,exception);
                semaphore.Release();
            }
        } 

        private Mutex mtx = new Mutex();
        private LinkedList<OpenRequest> openReqs = new LinkedList<OpenRequest>();
        private Smux.Session? smuxSession = null;
        public void Close()
        {
            Smux.Session? session = null;
            mtx.WaitOne();
            session = smuxSession;
            smuxSession = null;
            mtx.ReleaseMutex();
            if(session != null)
            {
                session.Close();   
            }
        }

        private void dial(Sanguo sanguo,Node node)
        {
            Task.Run(async () => {
                Socket s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                var ok = await node.connectAndLogin(sanguo,s,false);
                if(!ok)
                {   
                    mtx.WaitOne();
                    var e = new Exception("connect failed");
                    for(var node = openReqs.First;node != null;)
                    {
                        var req = node.Value;
                        openReqs.RemoveFirst();
                        req.OnOpenResponse(null,e);
                    }
                    mtx.ReleaseMutex();
                }
                else
                {
                    mtx.WaitOne();
                    var openReqs = this.openReqs;
                    this.openReqs = new LinkedList<OpenRequest>();
                    smuxSession = Smux.Session.Client(new NetworkStream(s,ownsSocket: true),new Smux.Config());
                    var session = smuxSession;
                    mtx.ReleaseMutex();
                    for(var node = openReqs.First;node != null;)
                    {
                        var req = node.Value;
                        openReqs.RemoveFirst();

                        try
                        {
                            var stream = await session.OpenStreamAsync();
                            req.OnOpenResponse(stream,null);
                        }
                        catch(Exception e)
                        {
                            req.OnOpenResponse(null,e);
                        } 
                    }
                }
            });         
        }

        public async Task<Smux.Stream> OpenStreamAsync(Sanguo sanguo,Node node)
        {
            mtx.WaitOne();
            if(smuxSession is null)
            {
                var openReq = new OpenRequest();
                openReqs.AddLast(openReq);
                if(openReqs.Count == 1)
                {
                    dial(sanguo,node);
                }
                mtx.ReleaseMutex();

                try
                {
                    await openReq.WaitAsync(sanguo.cancel.Token);
                }
                catch(OperationCanceledException)
                {
                    throw new Exception("sanguo is closed");
                }

                if(openReq.stream is null)
                {
                    throw new Exception("connect failed");
                }
                else 
                {
                    return openReq.stream;
                }
            } 
            else 
            {
                var session = smuxSession;
                mtx.ReleaseMutex();
                return await session.OpenStreamAsync();
            }
        }
    }
    

    private LinkedList<pendingMessage> pendingMsg = new LinkedList<pendingMessage>();
    private Session? session = null;
    private Mutex mtx = new Mutex();
    private StreamClient streamCli = new StreamClient();

    public Node(Addr addr,bool available,bool export):base(addr,available,export)
    {

    }

    public void closeSession()
    {
        mtx.WaitOne();
        if(!(session is null)){
            session.Close();
            session = null;
        }
        mtx.ReleaseMutex();
        streamCli.Close();
    }   

    public Task<Smux.Stream> OpenStreamAsync(Sanguo sanguo)
    {
        return streamCli.OpenStreamAsync(sanguo,this);
    }

    //清理已经超时或被取消的pendingMsg,返回pendingMsg中是否还有
    private void clearPendingMsg() 
    {
        var now = DateTime.Now;
        for(var node = pendingMsg.First;node != null;)
        {
            var msg = node.Value;
            if( msg.deadline != null && msg.deadline <= now) {
                //已经超时
                var next = node.Next;
                pendingMsg.Remove(node);
                node = next;
            } else if(msg.cancellationToken != null){
                CancellationToken token = (CancellationToken)msg.cancellationToken;
                if(token.IsCancellationRequested){
                    var next = node.Next;
                    pendingMsg.Remove(node);
                    node = next; 
                }
            } else {
                node = node.Next;
            }
        }
    }

    internal async Task<bool> connectAndLogin(Sanguo sanguo,Socket s,bool isStream)
    {
        using CancellationTokenSource cancellation = CancellationTokenSource.CreateLinkedTokenSource(sanguo.cancel.Token);
        cancellation.CancelAfter(5000);
        try{
            await s.ConnectAsync(Addr.IPEndPoint(),cancellation.Token);
            using MemoryStream jsonStream = new MemoryStream();
            JsonSerializer.Serialize(jsonStream,new SSLoginReq(sanguo.LocalAddr.LogicAddr.ToUint32(),sanguo.LocalAddr.NetAddr,isStream) ,typeof(SSLoginReq));
            var encryptJson = AES.CbcEncrypt(Sanguo.SecretKey,jsonStream.ToArray());
            using MemoryStream mstream = new MemoryStream(new byte[4+encryptJson.Length]);
            mstream.Write(BitConverter.GetBytes(Endian.Big(encryptJson.Length)));
            mstream.Write(encryptJson);
            var data = mstream.ToArray();  
            using NetworkStream nstream = new(s, ownsSocket: false);
            await nstream.WriteAsync(data,0,data.Length,cancellation.Token);
            var resp = new byte[4];    
            var n = await nstream.ReadAsync(resp,0,resp.Length,cancellation.Token);
            if(n < 4) {
                return false;
            } else {
                return true;
            }
        } 
        catch(Exception e)
        {   
            Console.WriteLine(e);
            return false;
        }
    } 



    private void dial(Sanguo sanguo)
    {
        Task.Run(async () => {
            for(;;){
                if(sanguo.cancel.IsCancellationRequested){
                    return;
                }
                Socket s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                var ok = await connectAndLogin(sanguo,s,false);
                if(ok) {
                    if(sanguo.cancel.IsCancellationRequested) {
                        s.Close();
                    } else {
                        mtx.WaitOne();
                        onEstablish(sanguo,s);
                        mtx.ReleaseMutex();
                    }
                    return;
                } else {
                    mtx.WaitOne();
                    clearPendingMsg();
                    if(pendingMsg.Count == 0) {
                        mtx.ReleaseMutex();
                        return;
                    } else {
                        mtx.ReleaseMutex();
                        continue;
                    }                    
                }
            }
        });         
    }

    public void SendMessage(Sanguo sanguo,ISSMsg msg,DateTime? deadline,CancellationToken? cancellationToken)
    {
        try {
            mtx.WaitOne();
            if(!(session is null)) {
                session.Send(msg);        
            } else {
                pendingMsg.AddLast(new pendingMessage(msg,deadline,cancellationToken));
                if(pendingMsg.Count == 1){
                    dial(sanguo);
                }
            } 
        }
        catch(Exception e)
        {
            Console.WriteLine(e);
        }
        finally
        {
            mtx.ReleaseMutex();
        }
    }

    private void onRelayMessage(Sanguo sanguo,RelayMessage msg)
    {
        Console.WriteLine($"onRelayMessage self:{sanguo.LocalAddr.LogicAddr.ToString()} {msg.From.ToString()}->{msg.To.ToString()}");
        var nextNode = sanguo.GetNodeByLogicAddr(msg.To);
        if(!(nextNode is null)) {
            nextNode.SendMessage(sanguo,msg,DateTime.Now.AddSeconds(1),null);
        } else {
            var rpcReq = msg.GetRpcRequest();
            //目标不可达，如果消息是RPC请求，向请求端返回不可达错误
            if(!(rpcReq is null) && !rpcReq.Oneway){
                nextNode = sanguo.GetNodeByLogicAddr(msg.From);
                if(!(nextNode is null)) {
                    var resp = new Rpc.Proto.rpcResponse();
                    resp.ErrCode = RpcError.ErrOther;
                    resp.ErrDesc = $"route message to target:{msg.To.ToString()} failed";
                    nextNode.SendMessage(sanguo,new RpcResponseMessage(msg.From,sanguo.LocalAddr.LogicAddr,resp),DateTime.Now.AddSeconds(1),null);
                }
            }
        }
    }

    private void onMessage(Sanguo sanguo,ISSMsg m)
    {
        if(m is SSMessage) {
            sanguo.DispatchMsg((SSMessage)m);
        } else if (m is RpcRequestMessage) {
            var request = (RpcRequestMessage)m;
            sanguo.OnRpcRequest(new rpcChannel(sanguo,this,request.From),request.Req);
        } else if (m is RpcResponseMessage) {
            sanguo.OnRpcResponse(((RpcResponseMessage)m).Resp);
        } else if (m is RelayMessage) {
            onRelayMessage(sanguo,(RelayMessage)m);
        }
    }

    public bool CheckConnection(Sanguo sanguo)
    {        
        var ok = true;
        mtx.WaitOne();
        if(pendingMsg.Count != 0) {
            if(sanguo.LocalAddr.LogicAddr.ToUint32() < Addr.LogicAddr.ToUint32()){
                ok = false;
            }

        } else if (session != null) {
            ok = false;
        }
        mtx.ReleaseMutex();
        return ok;
    }

    public void OnEstablish(Sanguo sanguo,Socket s)
    {   
        mtx.WaitOne();
        onEstablish(sanguo,s);
        mtx.ReleaseMutex();
    }

    private void onEstablish(Sanguo sanguo,Socket s) 
    {
        var codec = new SSMessageCodec(sanguo.LocalAddr.LogicAddr.ToUint32());
        var msgReveiver = new MessageReceiver(MessageConstont.MaxPacketSize,codec);
        session = new Session(s);
        session.SetCloseCallback((Session s) => {
            mtx.WaitOne();
            session = null;
            mtx.ReleaseMutex();
        });
        session.Start(msgReveiver,(Session s,Object packet) => {
            if(packet is ISSMsg) {
                if(this == sanguo.GetNodeByLogicAddr(Addr.LogicAddr)) {
                    onMessage(sanguo,(ISSMsg)packet);
                    return true;
                }
            }
            return false;
        });
        var now = DateTime.Now;
        for(var node = pendingMsg.First; node != null; node = pendingMsg.First){
            pendingMsg.Remove(node);
            var pending = node.Value;
            if(!(pending.deadline is null) && pending.deadline > now) {
                session.Send(pending.message);
            } else if (!(pending.cancellationToken is null)){
                CancellationToken token = (CancellationToken)pending.cancellationToken;
                if(!token.IsCancellationRequested){
                    session.Send(pending.message);
                }
            }
        }
    } 
}
