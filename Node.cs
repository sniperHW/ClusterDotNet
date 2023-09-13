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
namespace ClusterDotNet;

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
    private readonly object mtx = new object();
    public  LogicAddr localAddr;
    private int initOK = 0;
    private SemaphoreSlim semaphore = new SemaphoreSlim(0);
    private Random rnd = new Random();
    Dictionary<uint,Node> nodes = new Dictionary<uint,Node>();

    Dictionary<uint,ArrayList> nodeByType = new Dictionary<uint,ArrayList>();

    Dictionary<uint,ArrayList> harborsByCluster = new Dictionary<uint,ArrayList>();

    public NodeCache() 
    {
        localAddr = new LogicAddr();
    }

    public void WaitInit(CancellationToken cancellationToken) {
        semaphore.Wait(cancellationToken);
    }

    private void AddNodeByType(Node n)
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

    private void RemoveNodeByType(Node n)
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

    private void AddHarborsByCluster(Node n)
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

    private void RemoveHarborsByCluster(Node n)
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
        Node? n = null;
        lock(mtx)
        {
            if(harborsByCluster.ContainsKey(cluster))
            {
                ArrayList harbors = harborsByCluster[cluster];
                n = (Node?)harbors[rnd.Next(0,harbors.Count)];
            }
        }
        return n;
    }

    public Node? GetNodeByType(uint tt,int num)
    {
        Node? n = null;
        lock(mtx)
        {
            if(nodeByType.ContainsKey(tt)){
                ArrayList nodes = nodeByType[tt];
                if(num == 0) {
                    n = (Node?)nodes[rnd.Next(0,nodes.Count)];
                } else {
                    n = (Node?)nodes[num%nodes.Count];
                }
            }
        }
        return n;      
    }

    public Node? GetNodeByLogicAddr(LogicAddr logicAddr)
    {
        Node? n = null;
        lock(mtx)
        {
            if(nodes.ContainsKey(logicAddr.ToUint32())){
                n = nodes[logicAddr.ToUint32()];
            }
        }
        return n;        
    }

    public void Stop()
    {
       lock(mtx)
       {
            foreach( KeyValuePair<uint,Node> kvp in nodes ){
                kvp.Value.closeSession();
            }
       }
    }

    public void onNodeUpdate(ClusterNode self, DiscoveryNode []nodeinfo) {
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

        var removeSelf = false;
        lock(mtx)
        {
            Node[] localNodes = new Node[nodes.Count];
            var i = 0;
            var j = 0;
            foreach( KeyValuePair<uint,Node> kvp in nodes ){
                localNodes[i++] = kvp.Value;
            }
            Array.Sort(localNodes);
            i = 0;
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
        }

        if(removeSelf) {
            Task.Run(()=>self.Stop());

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

        private readonly object mtx = new object();
        private LinkedList<OpenRequest> openReqs = new LinkedList<OpenRequest>();
        private Smux.Session? smuxSession = null;
        public void Close()
        {
            Smux.Session? session = null;
            lock(mtx)
            {
                session = smuxSession;
                smuxSession = null;
            }
            if(session != null)
            {
                session.Close();   
            }
        }

        private void dial(ClusterNode self,Node node)
        {
            Task.Run(async () => {
                Socket s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                var ok = await node.connectAndLogin(self,s,false);
                if(!ok)
                {   
                    var e = new ClusterException("connect failed");
                    LinkedList<OpenRequest> openReqs;
                    lock(mtx)
                    {
                        openReqs = this.openReqs;
                        this.openReqs = new LinkedList<OpenRequest>();
                    }

                    for(var node = openReqs.First;node != null;)
                    {
                        var req = node.Value;
                        openReqs.RemoveFirst();
                        req.OnOpenResponse(null,e);
                    }
                }
                else
                {

                    Smux.Session session;
                    LinkedList<OpenRequest> openReqs;
                    lock(mtx)
                    {
                        openReqs = this.openReqs;
                        this.openReqs = new LinkedList<OpenRequest>();
                        smuxSession = Smux.Session.Client(new NetworkStream(s,ownsSocket: true),new Smux.Config());
                        session = smuxSession;
                    }

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

        public async Task<Smux.Stream> OpenStreamAsync(ClusterNode self,Node node)
        {

            Smux.Session? sess = null;
            var openReq = new OpenRequest();
            lock(mtx)
            {   
                sess = smuxSession;
                if(sess is null)
                {
                    openReqs.AddLast(openReq);
                    if(openReqs.Count == 1)
                    {
                        dial(self,node);
                    }                
                }
            }

            if(sess is null)
            {
                try
                {
                    await openReq.WaitAsync(self.die.Token);
                }
                catch(OperationCanceledException)
                {
                    throw new ClusterException("sanguo is closed");
                }

                if(openReq.stream is null)
                {
                    throw new ClusterException("connect failed");
                }
            }
            else 
            {
                openReq.stream = await sess.OpenStreamAsync();
            }

            return openReq.stream;
        }
    }
    

    private LinkedList<pendingMessage> pendingMsg = new LinkedList<pendingMessage>();
    private Session? session = null;
    private readonly object mtx = new object();
    private StreamClient streamCli = new StreamClient();

    public Node(Addr addr,bool available,bool export):base(addr,available,export)
    {

    }

    public void closeSession()
    {
        lock(mtx)
        {
            if(!(session is null)){
                session.Close();
                session = null;
            }
        }
        streamCli.Close();
    }   

    public Task<Smux.Stream> OpenStreamAsync(ClusterNode self)
    {
        return streamCli.OpenStreamAsync(self,this);
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

    internal async Task<bool> connectAndLogin(ClusterNode self,Socket s,bool isStream)
    {
        using CancellationTokenSource cancellation = CancellationTokenSource.CreateLinkedTokenSource(self.die.Token);
        cancellation.CancelAfter(5000);
        try{
            await s.ConnectAsync(Addr.IPEndPoint(),cancellation.Token);
            
            var jsonStr = JsonSerializer.Serialize(new SSLoginReq(self.LocalAddr.LogicAddr.ToUint32(),self.LocalAddr.NetAddr,isStream) ,typeof(SSLoginReq));
            var encryptJson = AES.CbcEncrypt(ClusterNode.SecretKey,Encoding.UTF8.GetBytes(jsonStr));
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



    private void dial(ClusterNode self)
    {
        Task.Run(async () => {
            for(;;){
                if(self.die.IsCancellationRequested){
                    return;
                }
                Socket s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                if(await connectAndLogin(self,s,false)) {
                    if(self.die.IsCancellationRequested) {
                        s.Close();
                    } else {
                        lock(mtx)
                        {
                            onEstablish(self,s);
                        }
                    }
                    return;
                } else {
                    var pendingCount = 0;
                    
                    lock(mtx)
                    {
                        clearPendingMsg();
                        pendingCount = pendingMsg.Count;
                    }

                    if(pendingCount==0){
                        return;
                    }                  
                }
            }
        });         
    }

    public void SendMessage(ClusterNode self,ISSMsg msg,DateTime? deadline,CancellationToken? cancellationToken)
    {

        lock(mtx)
        {
            try {
                if(!(session is null)) {
                    session.Send(msg);        
                } else {
                    pendingMsg.AddLast(new pendingMessage(msg,deadline,cancellationToken));
                    if(pendingMsg.Count == 1){
                        dial(self);
                    }
                } 
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    private void onRelayMessage(ClusterNode self,RelayMessage msg)
    {
        Console.WriteLine($"onRelayMessage self:{self.LocalAddr.LogicAddr.ToString()} {msg.From.ToString()}->{msg.To.ToString()}");
        var nextNode = self.GetNodeByLogicAddr(msg.To);
        if(!(nextNode is null)) {
            nextNode.SendMessage(self,msg,DateTime.Now.AddSeconds(1),null);
        } else {
            var rpcReq = msg.GetRpcRequest();
            //目标不可达，如果消息是RPC请求，向请求端返回不可达错误
            if(!(rpcReq is null) && !rpcReq.Oneway){
                nextNode = self.GetNodeByLogicAddr(msg.From);
                if(!(nextNode is null)) {
                    var resp = new rpcResponse();
                    resp.ErrCode = (short)RpcError.ErrOther;
                    resp.ErrStr = $"route message to target:{msg.To.ToString()} failed";
                    nextNode.SendMessage(self,new RpcResponseMessage(msg.From,self.LocalAddr.LogicAddr,resp),DateTime.Now.AddSeconds(1),null);
                }
            }
        }
    }

    private void onMessage(ClusterNode self,ISSMsg m)
    {
        if(m is SSMessage) {
            self.DispatchMsg((SSMessage)m);
        } else if (m is RpcRequestMessage) {
            var request = (RpcRequestMessage)m;
            self.OnRpcRequest(new rpcChannel(self,this,request.From),request.Req);
        } else if (m is RpcResponseMessage) {
            self.OnRpcResponse(((RpcResponseMessage)m).Resp);
        } else if (m is RelayMessage) {
            onRelayMessage(self,(RelayMessage)m);
        }
    }

    public bool CheckConnection(ClusterNode self)
    {        
        var ok = true;
        lock(mtx)
        {
            if(pendingMsg.Count != 0) {
                if(self.LocalAddr.LogicAddr.ToUint32() < Addr.LogicAddr.ToUint32()){
                    ok = false;
                }

            } else if (session != null) {
                ok = false;
            }
        }
        return ok;
    }

    public void OnEstablish(ClusterNode self,Socket s)
    {   
        lock(mtx)
        {
            onEstablish(self,s);
        }
    }

    private void onEstablish(ClusterNode self,Socket s) 
    {
        var codec = new SSMessageCodec(self.LocalAddr.LogicAddr.ToUint32());
        var msgReveiver = new MessageReceiver(MessageConstont.MaxPacketSize,codec);
        session = new Session(s);
        session.SetCloseCallback((Session s) => {
            lock(mtx)
            {
                session = null;
            }
        });
        session.Start(msgReveiver,(Session s,Object packet) => {
            if(packet is ISSMsg) {
                if(this == self.GetNodeByLogicAddr(Addr.LogicAddr)) {
                    onMessage(self,(ISSMsg)packet);
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
