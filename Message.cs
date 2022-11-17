using System;
//using System.Net;
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
                throw new SanguoException($"message:{message.Descriptor.Name} exists");
            } else if(idToMeta.ContainsKey(id)) {
                throw new SanguoException($"duplicate id:{id}");
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
                throw new SanguoException($"id:{id} not register");
            }

            Func<IMessage> factory = idToMeta[id];
            IMessage o = factory();
            using MemoryStream memoryStream = new MemoryStream(buff, offset, length);
            o.MergeFrom(memoryStream); 
            return o;
        }

        public void Marshal(IMessage message,MemoryStream stream)
        {
            if(!nameToID.ContainsKey(message.Descriptor.Name)) {
                throw new SanguoException($"message:{message.Descriptor.Name} not register");
            } else {
                message.WriteTo(stream);
            }
        }

        public int GetID(IMessage message)
        {
            if(!nameToID.ContainsKey(message.Descriptor.Name)) {
                throw new SanguoException($"message:{message.Descriptor.Name} not register");
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
            throw new SanguoException($"namespace:{ns} not exists");
        }
        return Namespace[ns].Unmarshal(id,buff,offset,length);
    }

    public static  void Marshal(string ns,IMessage message,MemoryStream stream) 
    {
        if(!Namespace.ContainsKey(ns)){
            throw new SanguoException($"namespace:{ns} not exists");
        }
        Namespace[ns].Marshal(message,stream);
    }

    public static int GetID(string ns,IMessage message) 
    {
        if(!Namespace.ContainsKey(ns)){
            throw new SanguoException($"namespace:{ns} not exists");
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
	public static  readonly int MaxPacketSize   = 1024 * 1024 * 4;

}

public class SSMessage : ISSMsg
{
    public ushort Cmd{get;}

    public LogicAddr To{get;}

    public LogicAddr From{get;}

    public IMessage Payload{get;}

    public void Encode(MemoryStream stream)
    {
        var position1 = stream.Position;
        try{
            stream.Write(BitConverter.GetBytes(0));//写入占位符
            byte flag = (byte)(0 | MessageConstont.Msg);
            stream.WriteByte(flag);
            stream.Write(BitConverter.GetBytes(Endian.Big((int)To.ToUint32())));
            stream.Write(BitConverter.GetBytes(Endian.Big((int)From.ToUint32())));
            stream.Write(BitConverter.GetBytes(Endian.Big((short)Cmd)));
            var position2 = stream.Position;
            ProtoMessage.Marshal("ss",Payload,stream);
            int pbSize = (int)(stream.Position - position2);
            var position3 = stream.Position;
            var payload = MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom + MessageConstont.SizeCmd + pbSize;
            if(payload+MessageConstont.SizeLen > MessageConstont.MaxPacketSize) {
                throw new SanguoException("packet too large");
            }
            stream.Position = position1;   
            stream.Write(BitConverter.GetBytes(Endian.Big(payload)));
            stream.Position = position3;
        } catch (Exception e) {
            Console.WriteLine(e);
            stream.Position = position1;
        }
    }


    public SSMessage(LogicAddr to,LogicAddr from,IMessage payload)
    {
        To = to;
        From = from;
        Payload = payload;
        Cmd = (ushort)ProtoMessage.GetID("ss",payload);
    }

    public SSMessage(LogicAddr to,LogicAddr from,ushort cmd,IMessage payload)
    {
        To = to;
        From = from;
        Payload = payload;
        Cmd = cmd;
    }

} 

public class RelayMessage : ISSMsg
{
    public LogicAddr To{get;}

    public LogicAddr From{get;}

    public byte[] Payload{get;}

    public Rpc.Proto.rpcRequest? GetRpcRequest(){
        if(((int)Payload[0] & MessageConstont.MaskMessageType) != MessageConstont.RpcReq){
            try
            {
                var offset = MessageConstont.SizeLen + MessageConstont.SizeFlag;
                using MemoryStream memoryStream = new MemoryStream(Payload, offset, Payload.Length - offset);
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
        To = to;
        From = from;
        Payload = payload;
    }

    public void Encode(MemoryStream stream)
    {
        var position1 = stream.Position;
        try{
            var lenBytes = BitConverter.GetBytes(Endian.Big(Payload.Length));
            stream.Write(lenBytes,0,lenBytes.Length);
            stream.Write(Payload,0,Payload.Length);
        } catch (Exception e) {
            Console.WriteLine(e);
            stream.Position = position1;
        }
    }
}


public class RpcRequestMessage : ISSMsg
{

    public LogicAddr To{get;}

    public LogicAddr From{get;}

    public  Rpc.Proto.rpcRequest Req{get;}

    public RpcRequestMessage(LogicAddr to,LogicAddr from,Rpc.Proto.rpcRequest req)
    {
        To = to;
        From = from;
        Req = req;
    }  

    public void Encode(MemoryStream stream)
    {
        var oriPos = stream.Position;
        var oriLength = stream.Length;
        try{
            stream.Write(BitConverter.GetBytes(0));//占位符
            stream.WriteByte((byte)(0 | MessageConstont.RpcReq));
            stream.Write(BitConverter.GetBytes(Endian.Big((int)To.ToUint32())));
            stream.Write(BitConverter.GetBytes(Endian.Big((int)From.ToUint32()))); 
            var pos2 = stream.Position;
            Req.WriteTo(stream);
            var pos3 = stream.Position;
            var payloadLen = MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom + (int)(pos3 - pos2);
            if(payloadLen+MessageConstont.SizeLen > MessageConstont.MaxPacketSize) {
                throw new SanguoException("packet too large");
            }
            stream.Position = oriPos;
            stream.Write(BitConverter.GetBytes(Endian.Big(payloadLen)));
            stream.Position = pos3;
        }  catch (Exception e) {
            Console.WriteLine(e);
            stream.Position = oriPos;
            stream.SetLength(oriPos);
        }
    }
}

public class RpcResponseMessage : ISSMsg
{

    public LogicAddr To{get;}

    public LogicAddr From{get;}

    public Rpc.Proto.rpcResponse Resp{get;}

    public RpcResponseMessage(LogicAddr to,LogicAddr from,Rpc.Proto.rpcResponse resp)
    {
        To = to;
        From = from;        
        Resp = resp;
    }

    public void Encode(MemoryStream stream)
    {
        var oriPos = stream.Position;
        var oriLength = stream.Length;
        try{
            stream.Write(BitConverter.GetBytes(0));//占位符
            stream.WriteByte((byte)(0 | MessageConstont.RpcResp));
            stream.Write(BitConverter.GetBytes(Endian.Big((int)To.ToUint32())));
            stream.Write(BitConverter.GetBytes(Endian.Big((int)From.ToUint32()))); 
            var pos2 = stream.Position;
            Resp.WriteTo(stream);
            var pos3 = stream.Position;
            var payloadLen = MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom + (int)(pos3 - pos2);
            if(payloadLen+MessageConstont.SizeLen > MessageConstont.MaxPacketSize) {
                throw new SanguoException("packet too large");
            }
            stream.Position = oriPos;
            stream.Write(BitConverter.GetBytes(Endian.Big(payloadLen)));
            stream.Position = pos3;
        }  catch (Exception e) {
            Console.WriteLine(e);
            stream.Position = oriPos;
            stream.SetLength(oriPos);
        }
    }
}


public interface MessageCodecI 
{
    public Object? Decode(byte[] buff,int offset,int length);
}

public class SSMessageCodec : MessageCodecI
{

    private uint localLogicAddr;

    public SSMessageCodec(uint localLogicAddr) 
    {
        this.localLogicAddr = localLogicAddr;
    }

    public Object? Decode(byte[] buff,int offset,int length)
    {
        var readpos = offset;
        var endpos = offset + length;
        if(length<MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom) {
            throw new SanguoException("invaild Message");
        }
        byte flag = buff[readpos];
        readpos += MessageConstont.SizeFlag;
        var to = new LogicAddr((uint)Endian.Big(BitConverter.ToInt32(buff, readpos)));
        readpos += MessageConstont.SizeLogicAddr;
        var from = new LogicAddr((uint)Endian.Big(BitConverter.ToInt32(buff, readpos)));
        readpos += MessageConstont.SizeLogicAddr;

        var msgType = (int)flag & MessageConstont.MaskMessageType;

        if(to.ToUint32() != localLogicAddr) {
            //当前节点非目的地
            return new RelayMessage(to,from,new Span<byte>(buff,offset,length).ToArray());
        } else if(msgType == MessageConstont.Msg) {
            if(buff.Length - readpos < MessageConstont.SizeCmd) {
                throw new SanguoException("invaild Message");
            }
            var cmd = Endian.Big((short)BitConverter.ToUInt16(buff, readpos));
            readpos += MessageConstont.SizeCmd;
            var msg = ProtoMessage.Unmarshal("ss",cmd,buff,readpos,endpos-readpos);
            return new SSMessage(to,from,(ushort)cmd,msg);
        } else if (msgType == MessageConstont.RpcReq) {
            using MemoryStream memoryStream = new MemoryStream(buff, readpos, endpos - readpos);
            Rpc.Proto.rpcRequest req = new Rpc.Proto.rpcRequest();
            req.MergeFrom(memoryStream); 
            return new RpcRequestMessage(to,from,req);
        } else if (msgType == MessageConstont.RpcResp) {
            using MemoryStream memoryStream = new MemoryStream(buff, readpos, endpos - readpos);
            Rpc.Proto.rpcResponse resp = new Rpc.Proto.rpcResponse();
            resp.MergeFrom(memoryStream);
            return new RpcResponseMessage(to,from,resp);
        } else {
            throw new SanguoException("invaild Message");
        }
    }
}

public class MessageReceiver: IPacketReceiver
{
    private byte[] buff;

    private int    w = 0;

    private int    r = 0;

    private MessageCodecI codec;

    private int maxPacketSize = 0; 

    public MessageReceiver(int maxPacketSize,MessageCodecI codec)
    {
        if(maxPacketSize <= 0) {
            throw new SanguoException("invaild MaxPacketSize");
        }
        this.maxPacketSize = maxPacketSize;
        buff = new byte[this.maxPacketSize];
        this.codec = codec;
    }

    public async Task<Object?> Recv(Func<byte[], int, int,Task<int>> recvfunc)
    {
        for(;;)
        {
            var unpackSize = w-r;
            if(unpackSize >= 4)
            {
                int payload =  Endian.Big(BitConverter.ToInt32(buff, r));
                int totalSize = payload+MessageConstont.SizeLen;

                if(payload <= 0)
                {
                    throw new SanguoException("invaild payload");
                } else if(totalSize > maxPacketSize){
                    throw new SanguoException("packet too large");
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

            var n = await recvfunc(buff, w, buff.Length-w);
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


