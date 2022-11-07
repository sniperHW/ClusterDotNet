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

        public IMessage Unmarshal(int id,byte[] buff,int offset,int length)
        {
            if(!idToMeta.ContainsKey(id)){
                throw new Exception($"id:{id} not register");
            }

            Func<IMessage> factory = idToMeta[id];
            IMessage o = factory();
            MemoryStream memoryStream = new MemoryStream(buff, offset, length);
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

    public static IMessage Unmarshal(string ns,int id,byte[] buff,int offset,int length)
    {
        if(!Namespace.ContainsKey(ns)){
            throw new Exception($"namespace:{ns} not exists");
        }
        return Namespace[ns].Unmarshal(id,buff,offset,length);
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
    public static  readonly int SizeLen       = sizeof(int); 
	public static  readonly int SizeFlag      = sizeof(byte);

    public static readonly  int SizeLogicAddr = sizeof(int);

	public static  readonly int SizeToAndFrom = SizeLogicAddr*2;
	public static  readonly int SizeCmd       = sizeof(ushort);
	public static  readonly int SizeRpcSeqNo  = sizeof(ulong);
	public static  readonly int MinSize       = SizeLen + SizeFlag + SizeToAndFrom;


    public static  readonly int Msg             = 0x8; 
	public static  readonly int RpcReq          = 0x10; 
	public static  readonly int RpcResp         = 0x18; 
	public static  readonly int MaskMessageType = 0x38;
	public static  readonly int MaxPacketSize   = 1024 * 4;

}

public class SSMessage : MessageI
{
	private ushort _cmd = 0;
    public ushort Cmd{get=>_cmd;}

    private LogicAddr _to;
    public LogicAddr To{get=>_to;}

    private LogicAddr _from;
    public LogicAddr From{get=>_from;}

    private IMessage _payload;
    public IMessage Payload{get=>_payload;}

    public void Encode(MemoryStream stream)
    {
        var position1 = stream.Position;
        try{
            stream.Write(BitConverter.GetBytes(0));//写入占位符
            byte flag = (byte)(0 | MessageConstont.Msg);
            stream.WriteByte(flag);
            stream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder((int)_to.ToUint32())));
            stream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder((int)_from.ToUint32())));
            stream.Write(BitConverter.GetBytes((UInt16)IPAddress.HostToNetworkOrder((int)_cmd)));
            var position2 = stream.Position;
            ProtoMessage.Marshal("ss",_payload,stream);
            var pbSize = stream.Position - position2;
            var position3 = stream.Position;
            stream.Position = position1;
            var sizePayload = MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom + MessageConstont.SizeCmd;
            if(sizePayload+MessageConstont.SizeLen > MessageConstont.MaxPacketSize) {
                throw new Exception("packet too large");
            }   
            stream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(sizePayload)));
            stream.Position = position3;
        } catch (Exception e) {
            Console.WriteLine(e);
            stream.Position = position1;
        }
    }


    public SSMessage(LogicAddr to,LogicAddr from,IMessage payload)
    {
        _to = to;
        _from = from;
        _payload = payload;
        _cmd = (ushort)ProtoMessage.GetID("ss",payload);
    }

    public SSMessage(LogicAddr to,LogicAddr from,ushort cmd,IMessage payload)
    {
        _to = to;
        _from = from;
        _payload = payload;
        _cmd = cmd;
    }

} 

public class RelayMessage : MessageI
{
    private LogicAddr _to;
    public LogicAddr To{get => _to;}

    private LogicAddr _from;
    public LogicAddr From{get => _from;}

    private byte[] _payload;
    public byte[] Payload{get => _payload;}

    public Rpc.Proto.rpcRequest? GetRpcRequest(){
        if(((int)_payload[0] & MessageConstont.MaskMessageType) != MessageConstont.RpcReq){
            try
            {
                var offset = MessageConstont.SizeLen + MessageConstont.SizeFlag;
                MemoryStream memoryStream = new MemoryStream(_payload, offset, _payload.Length - offset);
                Rpc.Proto.rpcRequest req = new Rpc.Proto.rpcRequest();
                req.MergeFrom(memoryStream); 
                return req;
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
        var position1 = stream.Position;
        try{
            var lenBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(_payload.Length));
            stream.Write(lenBytes,0,lenBytes.Length);
            stream.Write(_payload,0,_payload.Length);
        } catch (Exception e) {
            Console.WriteLine(e);
            stream.Position = position1;
        }
    }
}


public class RpcRequestMessage : MessageI
{

    private LogicAddr _to;
    public LogicAddr To{get=>_to;}

    private LogicAddr _from;
    public LogicAddr From{get=>_from;}

    private Rpc.Proto.rpcRequest _req;  

    public RpcRequestMessage(LogicAddr to,LogicAddr from,Rpc.Proto.rpcRequest req)
    {
        _to = to;
        _from = from;
        _req = req;
    }  

    public void Encode(MemoryStream stream)
    {
        var position1 = stream.Position;
        try{
            MemoryStream tmpstream = new MemoryStream();
            _req.WriteTo(tmpstream);
            var payloadLen = MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom + (int)tmpstream.Length;
			var totalLen = MessageConstont.SizeLen + payloadLen;
            if(payloadLen+MessageConstont.SizeLen > MessageConstont.MaxPacketSize) {
                throw new Exception("packet too large");
            }
            stream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(payloadLen)));
            byte flag = (byte)(0 | MessageConstont.RpcReq);
            stream.WriteByte(flag);
            stream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder((int)_to.ToUint32())));
            stream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder((int)_from.ToUint32()))); 
            stream.Write(tmpstream.ToArray());   
        }  catch (Exception e) {
            Console.WriteLine(e);
            stream.Position = position1;
        }
    }
}

public class RpcResponseMessage : MessageI
{

    private LogicAddr _to;
    public LogicAddr To{get=>_to;}

    private LogicAddr _from;
    public LogicAddr From{get=>_from;}

    private Rpc.Proto.rpcResponse _resp;

    public Rpc.Proto.rpcResponse Resp{get=>_resp;}

    public RpcResponseMessage(LogicAddr to,LogicAddr from,Rpc.Proto.rpcResponse resp)
    {
        _to = to;
        _from = from;        
        _resp = resp;
    }

    public void Encode(MemoryStream stream)
    {
        var position1 = stream.Position;
        try{
            MemoryStream tmpstream = new MemoryStream();
            _resp.WriteTo(tmpstream);
            var payloadLen = MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom + (int)tmpstream.Length;
			var totalLen = MessageConstont.SizeLen + payloadLen;
            if(payloadLen+MessageConstont.SizeLen > MessageConstont.MaxPacketSize) {
                throw new Exception("packet too large");
            }
            stream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(payloadLen)));
            byte flag = (byte)(0 | MessageConstont.RpcResp);
            stream.WriteByte(flag);
            stream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder((int)_to.ToUint32())));
            stream.Write(BitConverter.GetBytes(IPAddress.HostToNetworkOrder((int)_from.ToUint32()))); 
            stream.Write(tmpstream.ToArray());   
        }  catch (Exception e) {
            Console.WriteLine(e);
            stream.Position = position1;
        }
    }
}


public interface MessageCodecI 
{
    public Object? Decode(byte[] buff,int offset,int length);
}

public class SSMessageCodec
{

    private uint localLogicAddr;

    public SSMessageCodec(uint localLogicAddr) 
    {
        this.localLogicAddr = localLogicAddr;
    }

    public Object? Decode(byte[] buff,int offset,int length)
    {
        var backOffset = offset;
        var endOffset = offset + length;
        if(length<MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom) {
            throw new SystemException("invaild Message");
        }
        byte flag = buff[offset];
        offset += MessageConstont.SizeFlag;
        uint to = (uint)IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buff, offset));
        offset += MessageConstont.SizeLogicAddr;
        uint from = (uint)IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buff, offset));
        offset += MessageConstont.SizeLogicAddr;
        
        var msgType = (int)flag & MessageConstont.MaskMessageType;

        if(to != localLogicAddr) {
            //当前节点非目的地
            byte[] payload = new byte[length];
            Buffer.BlockCopy(buff,backOffset,payload,0,length);
            return new RelayMessage(new LogicAddr(to),new LogicAddr(from),payload);
        } else if(msgType == MessageConstont.Msg) {
            if(buff.Length - offset < MessageConstont.SizeCmd) {
                throw new SystemException("invaild Message");
            }
            var cmd = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buff, offset));
            offset += MessageConstont.SizeCmd;
            var msg = ProtoMessage.Unmarshal("ss",cmd,buff,offset,endOffset-offset);
            return new SSMessage(new LogicAddr(to),new LogicAddr(from),(ushort)cmd,msg);
        } else if (msgType == MessageConstont.RpcReq) {
            MemoryStream memoryStream = new MemoryStream(buff, offset, endOffset - offset);
            Rpc.Proto.rpcRequest req = new Rpc.Proto.rpcRequest();
            req.MergeFrom(memoryStream); 
            return new RpcRequestMessage(new LogicAddr(to),new LogicAddr(from),req);
        } else if (msgType == MessageConstont.RpcResp) {
            MemoryStream memoryStream = new MemoryStream(buff, offset, endOffset - offset);
            Rpc.Proto.rpcResponse resp = new Rpc.Proto.rpcResponse();
            resp.MergeFrom(memoryStream);
            return new RpcResponseMessage(new LogicAddr(to),new LogicAddr(from),resp);
        } else {
            throw new SystemException("invaild Message");
        }
    }
}

public class MessageReceiver: PacketReceiverI
{
    private byte[] buff;
    private int    w = 0;

    private int    r = 0;

    private MessageCodecI codec;

    private int maxPacketSize = 0; 

    public MessageReceiver(int maxPacketSize,MessageCodecI codec)
    {
        if(maxPacketSize <= 0) {
            throw new Exception("invaild MaxPacketSize");
        }
        this.maxPacketSize = maxPacketSize;
        buff = new byte[this.maxPacketSize];
        this.codec = codec;
    }

    public async Task<Object?> Recv(StreamReaderI reader)
    {
        for(;;)
        {
            var unpackSize = w-r;
            if(unpackSize >= 4)
            {
                int payload =  IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buff, r));
                int totalSize = payload+MessageConstont.SizeLen;
                if(payload <= 0)
                {
                    throw new SystemException("invaild payload");
                } else if(totalSize > maxPacketSize){
                    throw new SystemException("packet too large");
                } else if(totalSize <= unpackSize) {
                    r += MessageConstont.SizeLen;
                    Object? msg = codec.Decode(buff,r,payload);
                    r += payload;
                    if(r == w){
                        r = w = 0;
                    } 
                    return msg;
                } else {
                    Buffer.BlockCopy(buff,r,buff,0,w-r);
                    w = w - r;
                    r = 0;
                }
            }

            var n = await reader.Recv(buff, w, buff.Length-w);
            if(n == 0)
            {
                return null;
            } else
            {
                w+=n;
            } 
        }
    }
}


