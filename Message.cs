using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
using Google.Protobuf;
namespace SanguoDotNet;


public class ProtoMessage
{
    private class Meta
    {
        public Dictionary<string,int> nameToID = new Dictionary<string,int>();

        public Dictionary<int,Func<IMessage>> idToMeta = new Dictionary<int,Func<IMessage>>();

        public void Register<T>(int id,T message) where T : IMessage<T>,new()
        {
            if(nameToID.ContainsKey(message.Descriptor.Name)) {
                throw new Exception($"message:{message.Descriptor.Name} exists");
            } else if(idToMeta.ContainsKey(id)) {
                throw new Exception($"duplicate id:{id}");
            } else {
                nameToID[message.Descriptor.Name] = id;
                idToMeta[id] = () => {
                    return new T();
                };
            }
        }

        public IMessage Unmarshal(int id,byte[] buff)
        {
            if(!idToMeta.ContainsKey(id)){
                throw new Exception($"id:{id} not register");
            }

            Func<IMessage> factory = idToMeta[id];
            IMessage o = factory();
            MemoryStream memoryStream = new MemoryStream(buff, 0, buff.Length);
            o.MergeFrom(memoryStream); 
            return o;
        }

        public void Marshal(IMessage message,MemoryStream stream)
        {
            if(!nameToID.ContainsKey(message.Descriptor.Name)) {
                throw new Exception($"message:{message.Descriptor.Name} not register");
            } else {
                message.WriteTo(stream);
            }
        }

        public int GetID(IMessage message)
        {
            if(!nameToID.ContainsKey(message.Descriptor.Name)) {
                throw new Exception($"message:{message.Descriptor.Name} not register");
            } else {
                return nameToID[message.Descriptor.Name];
            }            
        }
    }

    static Dictionary<string,Meta> Namespace = new Dictionary<string,Meta>() ;

    public static void Register<T>(string ns, int id,T message) where T : IMessage<T>,new()
    {
        Meta meta;
        if(Namespace.ContainsKey(ns)){
            meta = Namespace[ns];
        } else {
            meta = new Meta();
            Namespace[ns] = meta;
        }

        meta.Register<T>(id,message);    
    }

    public static IMessage Unmarshal(string ns,int id,byte[] buff)
    {
        if(!Namespace.ContainsKey(ns)){
            throw new Exception($"namespace:{ns} not exists");
        }
        return Namespace[ns].Unmarshal(id,buff);
    }

    public static  void Marshal(string ns,IMessage message,MemoryStream stream) 
    {
        if(!Namespace.ContainsKey(ns)){
            throw new Exception($"namespace:{ns} not exists");
        }
        Namespace[ns].Marshal(message,stream);
    }

    public static int GetID(string ns,IMessage message) 
    {
        if(!Namespace.ContainsKey(ns)){
            throw new Exception($"namespace:{ns} not exists");
        }
        return Namespace[ns].GetID(message);        
    }
}

public class MessageConstont 
{
    public static  readonly int SizeLen       = 4; 
	public static  readonly int SizeFlag      = 1;
	public static  readonly int SizeToAndFrom = 8;
	public static  readonly int SizeCmd       = 2;
	public static  readonly int SizeRpcSeqNo  = 8;
	public static  readonly int MinSize       = SizeLen + SizeFlag + SizeToAndFrom;


    public static  readonly int Msg             = 0x8; 
	public static  readonly int RpcReq          = 0x10; 
	public static  readonly int RpcResp         = 0x18; 
	public static  readonly int MaskMessageType = 0x38;
	public static  readonly int MaxPacketSize   = 1024 * 4;

}

public class Message : MessageI
{
	private UInt16 _cmd = 0;
    public UInt16 Cmd{get=>_cmd;}

    private LogicAddr _to;
    public LogicAddr To{get=>_to;}

    private LogicAddr _from;
    public LogicAddr From{get=>_from;}

    private Object? _payload;
    public Object? Payload{get=>_payload;}

    public void Encode(MemoryStream stream)
    {
    }

    public Message(LogicAddr to,LogicAddr from,Object payload)
    {
        _to = to;
        _from = from;
        _payload = payload;
    }


    public Message(LogicAddr to,LogicAddr from,UInt16 cmd, Object payload)
    {
        _cmd = cmd;
        _to = to;
        _from = from;
        _payload = payload;
    }

} 

public class RelayMessage
{
    private LogicAddr _to;
    public LogicAddr To{get => _to;}

    private LogicAddr _from;
    public LogicAddr From{get => _from;}

    private byte[] _payload;
    public byte[] Payload{get => _payload;}

    public RpcRequestMessage? GetRpcRequest(){
        if(((int)_payload[0] & MessageConstont.MaskMessageType) != MessageConstont.RpcReq){
            try
            {
                MemoryStream memoryStream = new MemoryStream(_payload, 5, _payload.Length - 5);
                Rpc.Proto.rpcRequest req = new Rpc.Proto.rpcRequest();
                req.MergeFrom(memoryStream); 
                return new RpcRequestMessage(req.Seq,req.Method,req.Arg.ToByteArray());
            }
            catch (Exception)
            {
                return null;
            }
        } else {
            return null;
        }
    }

    public RelayMessage(LogicAddr to,LogicAddr from,byte[] payload)
    {
        _to = to;
        _from = from;
        _payload = payload;
    }

    public void Encode(MemoryStream stream)
    {
        var lenBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(_payload.Length));
        stream.Write(lenBytes,0,lenBytes.Length);
        stream.Write(_payload,0,_payload.Length);
    }
}


