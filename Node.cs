using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
using System.Collections;
using System.Collections.Generic;
namespace SanguoDotNet;

public class DiscoveryNode
{
    private Addr _addr;
    public Addr Addr{get=>_addr;set=>_addr=value;}
    private bool _export;
    public bool Export{get=>_export;set=>_export=value;}
    private bool _available;
    public bool Available{get=>_available;set=>_available=value;}

    public DiscoveryNode(Addr addr,bool export,bool available)
    {
        _addr = addr;
        _export = export;
        _available = available;
    }

}

public interface DiscoveryI
{
    void Subscribe(Action<DiscoveryNode[]> onUpdate);
}


public class NodeCache
{
    private Mutex mtx = new Mutex();
    private LogicAddr localAddr;
    private int initOK = 0;
    private SemaphoreSlim semaphore = new SemaphoreSlim(1);

    private Action? _onSelfRemove = null;

    private Random rnd = new Random();

    public  Action? OnSelfRemove{
        get => Interlocked.Exchange(ref _onSelfRemove,_onSelfRemove);
        set => Interlocked.Exchange(ref _onSelfRemove,value);
    }

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
        ArrayList? nodeByType = this.nodeByType[n.Addr.LogicAddr.Type()];
        if(nodeByType is null){
            nodeByType = new ArrayList();
            nodeByType.Add(n);
            this.nodeByType[n.Addr.LogicAddr.Type()] = nodeByType;
        } else {
            nodeByType.Add(n);
        }
    }

    public void RemoveNodeByType(Node n)
    {
        ArrayList? nodeByType = this.nodeByType[n.Addr.LogicAddr.Type()];
        if(!(nodeByType is null)){
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
        ArrayList? harborsByCluster = this.harborsByCluster[n.Addr.LogicAddr.Cluster()];
        if(harborsByCluster is null){
            harborsByCluster = new ArrayList();
            harborsByCluster.Add(n);
            this.harborsByCluster[n.Addr.LogicAddr.Cluster()] = harborsByCluster;
        } else {
            harborsByCluster.Add(n);
        }
    }

    public void RemoveHarborsByCluster(Node n)
    {
        ArrayList? harborsByCluster = this.harborsByCluster[n.Addr.LogicAddr.Cluster()];
        if(!(harborsByCluster is null)){
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
        ArrayList? harbors = harborsByCluster[cluster];
        if(harbors is null)
        {
            mtx.ReleaseMutex();
            return null;
        } else {
            var n = (Node?)harbors[rnd.Next(0,harbors.Count)];
            mtx.ReleaseMutex();
            return n;
        }
    }

    public Node? GetNodeByType(uint tt,int num)
    {
        mtx.WaitOne();
        ArrayList? nodes = nodeByType[tt];
        if(nodes is null)
        {
            mtx.ReleaseMutex();
            return null;
        } else {
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
        n = nodes[logicAddr.ToUint32()];
        mtx.ReleaseMutex();
        return n;        
    }

    private class ReverserClass2 : IComparer
    {
      public int Compare(Object? x, Object? y)
      {
          var xx = (Node?)x;
          var yy = (Node?)y;
          if(xx is null) {
            return -1; 
          } else if (yy is null){
            return -1;
          }else if(xx.Addr.LogicAddr.ToUint32() < yy.Addr.LogicAddr.ToUint32()){
            return -1;
          } else if (xx.Addr.LogicAddr.ToUint32() == yy.Addr.LogicAddr.ToUint32()){
            return 0;
          } else {
            return 1;
          }
      }
    }

    private class ReverserClass1 : IComparer
    {
      public int Compare(Object? x, Object? y)
      {
          var xx = (DiscoveryNode?)x;
          var yy = (DiscoveryNode?)y;
          if(xx is null) {
            return -1; 
          } else if (yy is null){
            return -1;
          }else if(xx.Addr.LogicAddr.ToUint32() < yy.Addr.LogicAddr.ToUint32()){
            return -1;
          } else if (xx.Addr.LogicAddr.ToUint32() == yy.Addr.LogicAddr.ToUint32()){
            return 0;
          } else {
            return 1;
          }
      }
    }


    public void onNodeUpdate(DiscoveryNode []nodeinfo) {
        ArrayList interested = new ArrayList();
        for(var k = 0;k < nodeinfo.Length;k++){
            DiscoveryNode v = nodeinfo[k];
            if(v.Export || v.Addr.LogicAddr.Cluster() == localAddr.Cluster()){
                interested.Add(v);
            } else if(localAddr.Type() == LogicAddr.HarbarType && v.Addr.LogicAddr.Type() == LogicAddr.HarbarType){
                interested.Add(v);
            }
        }
        interested.Sort(new ReverserClass1());
        DiscoveryNode[] nodesFromDiscovery = (DiscoveryNode[])interested.ToArray(typeof(DiscoveryNode));

        mtx.WaitOne();

        Node[] localNodes = new Node[nodes.Count];
        var i = 0;
        var j = 0;
        foreach( KeyValuePair<uint,Node> kvp in nodes ){
            localNodes[i++] = kvp.Value;
        }
        Array.Sort(localNodes,new ReverserClass2());
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
                Node n = new Node(updateNode.Addr,updateNode.Available);
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

        if(removeSelf) {
            mtx.ReleaseMutex();
            if(!(OnSelfRemove is null)){
                OnSelfRemove();
            }
        }

        for(; i < nodesFromDiscovery.Length; i++) {
            var updateNode = (DiscoveryNode)nodesFromDiscovery[i];
            Node n = new Node(updateNode.Addr,updateNode.Available);
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

        mtx.ReleaseMutex();

        if(removeSelf) {
            if(!(OnSelfRemove is null)){
                OnSelfRemove();
            }
        } else {
            if(Interlocked.CompareExchange(ref initOK,1,0) == 0){
                semaphore.Release();
            }
        }
    }    
}

public class Node
{

    private class pendingMessage
    {
        public MessageI message;
        public DateTime? deadline;
        public CancellationToken? cancellationToken;

        public pendingMessage(MessageI message,DateTime? deadline,CancellationToken? cancellationToken)
        {
            this.message = message;
            this.deadline = deadline;
            this.cancellationToken = cancellationToken;
        }

    }

    private LinkedList<pendingMessage> pendingMsg = new LinkedList<pendingMessage>();

    private Session? session = null;

    private bool dialing = false;

    private Mutex mtx = new Mutex();

    private Addr _addr;

    public Addr Addr{get=>_addr;}

    private bool _available;

    public bool Available{get=>_available;set=>_available=value;}

    public Node(Addr addr,bool available) {
        _addr = addr;
        _available = available;
    }

    public void closeSession()
    {
        mtx.WaitOne();
        if(!(session is null)){
            session.Close();
            session = null;
        }
        mtx.ReleaseMutex();
    }

    private void dial(Sanguo sanguo)
    {

    }

    public void SendMessage(Sanguo sanguo,MessageI msg,DateTime? deadline,CancellationToken? cancellationToken)
    {
        try {
            mtx.WaitOne();
            if(!(session is null)) {
                session.Send(msg);        
            } else {
                pendingMsg.AddLast(new pendingMessage(msg,deadline,cancellationToken));
                if(!dialing){
                    dialing = true;
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

    private void onMessage(Sanguo sanguo,MessageI m)
    {

    } 
    
    /*func (n *node) onMessage(sanguo *Sanguo, msg interface{}) {
	switch msg := msg.(type) {
	case *ss.Message:
		switch m := msg.Payload().(type) {
		case proto.Message:
			sanguo.dispatchMessage(msg.From(), msg.Cmd(), m)
		case *rpcgo.RequestMsg:
			sanguo.rpcSvr.OnMessage(context.TODO(), &rpcChannel{peer: msg.From(), node: n, sanguo: sanguo}, m)
		case *rpcgo.ResponseMsg:
			sanguo.rpcCli.OnMessage(context.TODO(), m)
		}
	case *ss.RelayMessage:
		n.onRelayMessage(sanguo, msg)
	}
}*/

}
