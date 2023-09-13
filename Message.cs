using System;
//using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
using Google.Protobuf;
namespace ClusterDotNet;


public class ProtoMessage
{
    private class Meta
    {
        public Dictionary<string,int> nameToID = new Dictionary<string,int>();

        public Dictionary<int,Func<IMessage>> idToMeta = new Dictionary<int,Func<IMessage>>();

        public void Register<T>(int id,T message) where T : IMessage<T>,new()
        {
            if(nameToID.ContainsKey(message.Descriptor.Name)) {
                throw new ClusterException($"message:{message.Descriptor.Name} exists");
            } else if(idToMeta.ContainsKey(id)) {
                throw new ClusterException($"duplicate id:{id}");
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
                throw new ClusterException($"id:{id} not register");
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
                throw new ClusterException($"message:{message.Descriptor.Name} not register");
            } else {
                message.WriteTo(stream);
            }
        }

        public int GetID(IMessage message)
        {
            if(!nameToID.ContainsKey(message.Descriptor.Name)) {
                throw new ClusterException($"message:{message.Descriptor.Name} not register");
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
            throw new ClusterException($"namespace:{ns} not exists");
        }
        return Namespace[ns].Unmarshal(id,buff,offset,length);
    }

    public static  void Marshal(string ns,IMessage message,MemoryStream stream) 
    {
        if(!Namespace.ContainsKey(ns)){
            throw new ClusterException($"namespace:{ns} not exists");
        }
        Namespace[ns].Marshal(message,stream);
    }

    public static int GetID(string ns,IMessage message) 
    {
        if(!Namespace.ContainsKey(ns)){
            throw new ClusterException($"namespace:{ns} not exists");
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


    public static  readonly int PbMsg             = 0x1; 
    public static  readonly int BinMsg            = 0x2; 
	public static  readonly int RpcReq            = 0x3; 
	public static  readonly int RpcResp           = 0x4; 
	public static  readonly int MaskMessageType   = 0x7;
	public static  readonly int MaxPacketSize     = 1024 * 1024 * 4;
}

public class RPCMessageConstont 
{
    public static  readonly int LenSeq       = 8; 
	public static  readonly int LenOneWay    = 1;
    public static readonly  int LenMethod    = 2;
    public static readonly int MaxMethodLen = 65535;
    public static readonly int ReqHdrLen = LenSeq + LenOneWay + LenMethod; // seq + oneway + len(method)
    public static readonly int MaxErrStrLen = 65535;
    public static readonly int LenErrCode = 2;
    public static readonly int RespHdrLen = LenSeq + LenErrCode; //seq + Error.Err.Code
    public static readonly int LenErrStr = 2;
}

public class rpcRequest 
{
    public ulong Seq{get;set;}

    public bool   Oneway{get;set;}

    public string  Method{get;set;}

    public byte[]  Arg{get;set;}

    public rpcRequest()
    {
        Method="";
        Arg=new byte[0];
    }

    public rpcRequest(ulong seq,bool oneway,string method,byte[] arg)
    {
        if(method.Length > RPCMessageConstont.MaxMethodLen)
        {
            throw new ClusterException("method too large");
        }

        Seq = seq;
        Oneway = oneway;
        Method = method;
        Arg = arg;
    }

    //编码后的二进制大小
    public int EncodeLen() 
    {
        return  RPCMessageConstont.ReqHdrLen + Method.Length + Arg.Length;       
    }
    public void Encode(MemoryStream stream)
    {
        stream.Write(BitConverter.GetBytes(Endian.Big(Seq)));
        if(Oneway) {
            stream.WriteByte((byte)(1));
        }
        else 
        {
            stream.WriteByte((byte)(0));            
        }
        stream.Write(BitConverter.GetBytes(Endian.Big((short)Method.Length)));
        stream.Write(System.Text.Encoding.ASCII.GetBytes(Method));  
        stream.Write(Arg);     
    }

    public void Decode(byte[] buff,int offset,int endpos)
    {
        if(endpos - offset >= 8) 
        {
            Seq = Endian.Big((ulong)BitConverter.ToInt64(buff, offset));
            offset += 8;
        }

        if(endpos - offset >= 1) 
        {
            Oneway = (int)buff[offset] == 1;
            offset += 1;
        }

        if(endpos - offset >= 2) 
        {
            int lenMthod = (int)Endian.Big((short)BitConverter.ToUInt16(buff, offset));
            offset += 2;
            if(endpos - offset >= lenMthod) {
                char[] asciiChars = new char[System.Text.Encoding.ASCII.GetCharCount(buff, offset, lenMthod)];
                System.Text.Encoding.ASCII.GetChars(buff, offset, lenMthod, asciiChars, 0);
                Method = new string(asciiChars);
                offset += lenMthod;       
            }
        }

        if(endpos - offset > 0)
        {
            Arg = new byte[buff.Length - offset];
            Array.Copy(buff,offset,Arg,0,endpos-offset);
        }
    }
}


public class rpcResponse
{
    public ulong  Seq{get;set;}

    public short  ErrCode{get;set;}

    public string ErrStr{get;set;}

    public byte[] Ret{get;set;}


    public rpcResponse()
    {
        ErrStr = "";
        Ret = new byte[0];
    }

    public rpcResponse(ulong seq,byte[] ret) 
    {
        Seq = seq;
        Ret = ret;
        ErrStr = "";
    }    

    public rpcResponse(ulong seq,short errCode,string errStr)
    {
        Seq = seq;
        ErrCode = errCode;
        ErrStr = errStr;
        Ret = new byte[0];
    }

    public int EncodeLen() 
    {
        if(ErrCode == 0)
        {
            return RPCMessageConstont.RespHdrLen + Ret.Length;
        } 
        else 
        {
            return RPCMessageConstont.RespHdrLen + RPCMessageConstont.LenErrStr + ErrStr.Length;
        }     
    }

    public void Encode(MemoryStream stream)
    {
        stream.Write(BitConverter.GetBytes(Endian.Big(Seq)));
        stream.Write(BitConverter.GetBytes(Endian.Big((short)ErrCode)));
        if(ErrCode == 0)
        {
            stream.Write(Ret);
        }
        else 
        {
            stream.Write(BitConverter.GetBytes(Endian.Big((short)ErrStr.Length)));
            stream.Write(System.Text.Encoding.ASCII.GetBytes(ErrStr)); 
        }
    }

    public void Decode(byte[] buff,int offset,int endpos)
    {
        if(endpos - offset >= 8) 
        {
            Seq = Endian.Big((ulong)BitConverter.ToInt64(buff, offset));
            offset += 8;
        }

        if(endpos - offset >= 2)
        {
            ErrCode = Endian.Big((short)BitConverter.ToUInt16(buff, offset));
            offset += 2;
        }

        if(ErrCode == 0)
        {
            if(endpos - offset > 0)
            {
                Ret = new byte[endpos - offset];
                Array.Copy(buff,offset,Ret,0,endpos-offset);
            }    
        } 
        else 
        {
            int lenErrStr = (int)Endian.Big((short)BitConverter.ToUInt16(buff, offset));
            offset += 2;
            if(endpos - offset >= lenErrStr) {
                char[] asciiChars = new char[System.Text.Encoding.ASCII.GetCharCount(buff, offset, lenErrStr)];
                System.Text.Encoding.ASCII.GetChars(buff, offset, lenErrStr, asciiChars, 0);
                ErrStr = new string(asciiChars);     
            }
        }
    }
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
            byte flag = (byte)(0 | MessageConstont.PbMsg);
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
                throw new ClusterException("packet too large");
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

    public rpcRequest? GetRpcRequest(){
        if(((int)Payload[0] & MessageConstont.MaskMessageType) != MessageConstont.RpcReq){
            try
            {
                var offset = MessageConstont.SizeLen + MessageConstont.SizeFlag;
                rpcRequest req = new rpcRequest();
                req.Decode(Payload,offset,Payload.Length);
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

    public rpcRequest Req{get;}

    public RpcRequestMessage(LogicAddr to,LogicAddr from,rpcRequest req)
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
            var payloadLen = MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom + Req.EncodeLen();
            if(payloadLen+MessageConstont.SizeLen > MessageConstont.MaxPacketSize) {
                throw new ClusterException("packet too large");
            }       
            stream.Write(BitConverter.GetBytes(Endian.Big(payloadLen)));     
            stream.WriteByte((byte)(0 | MessageConstont.RpcReq));
            stream.Write(BitConverter.GetBytes(Endian.Big((int)To.ToUint32())));
            stream.Write(BitConverter.GetBytes(Endian.Big((int)From.ToUint32()))); 
            Req.Encode(stream);
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

    public rpcResponse Resp{get;}

    public RpcResponseMessage(LogicAddr to,LogicAddr from,rpcResponse resp)
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

            var payloadLen = MessageConstont.SizeFlag + MessageConstont.SizeToAndFrom + Resp.EncodeLen();
            if(payloadLen+MessageConstont.SizeLen > MessageConstont.MaxPacketSize) {
                throw new ClusterException("packet too large");
            }

            stream.Write(BitConverter.GetBytes(Endian.Big(payloadLen)));
            stream.WriteByte((byte)(0 | MessageConstont.RpcResp));
            stream.Write(BitConverter.GetBytes(Endian.Big((int)To.ToUint32())));
            stream.Write(BitConverter.GetBytes(Endian.Big((int)From.ToUint32()))); 
            Resp.Encode(stream);            
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
            throw new ClusterException("invaild Message");
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
        } else if(msgType == MessageConstont.PbMsg) {
            if(buff.Length - readpos < MessageConstont.SizeCmd) {
                throw new ClusterException("invaild Message");
            }
            var cmd = Endian.Big((short)BitConverter.ToUInt16(buff, readpos));
            readpos += MessageConstont.SizeCmd;
            var msg = ProtoMessage.Unmarshal("ss",cmd,buff,readpos,endpos-readpos);
            return new SSMessage(to,from,(ushort)cmd,msg);
        } else if (msgType == MessageConstont.RpcReq) {
            rpcRequest req = new rpcRequest();
            req.Decode(buff,readpos,endpos); 
            return new RpcRequestMessage(to,from,req);
        } else if (msgType == MessageConstont.RpcResp) {
            rpcResponse resp = new rpcResponse();
            resp.Decode(buff,readpos,endpos);
            return new RpcResponseMessage(to,from,resp);
        } else {
            throw new ClusterException("invaild Message");
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
            throw new ClusterException("invaild MaxPacketSize");
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
                    throw new ClusterException("invaild payload");
                } else if(totalSize > maxPacketSize){
                    throw new ClusterException("packet too large");
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


