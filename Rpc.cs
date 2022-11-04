using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;
namespace SanguoDotNet;

public class RpcRequestMessage 
{
    private UInt64 _seqno;
    public UInt64 Seqno{get=>_seqno;}

    private string _method;
    public string Method{get=>_method;}

    private byte[] _arg;
    public byte[] Arg{get=>_arg;}

    private bool _oneway = false;
    public bool   Oneway{get=>_oneway;}   

    public RpcRequestMessage(UInt64 seqno,string method,byte[] arg)
    {
        _seqno = seqno;
        _method = method;
        _arg = arg;
    }  

    public RpcRequestMessage(UInt64 seqno,string method,byte[] arg,bool oneway)
    {
        _seqno = seqno;
        _method = method;
        _arg = arg;
        _oneway = oneway;
    }

}

struct RpcResponseMessage
{

    public class RpcError
    {
        private int _code = 0;
        public int Code{get=>_code;}

        private string? _desc = null;
        public string? Desc{get=>_desc;}

        public RpcError(int code,string desc)
        {
            _code = code;
            _desc = desc;
        }

    }

    private UInt64 _seqno;
    public UInt64 Seqno{get=>_seqno;}

    private byte[]? _ret = null;
    public byte[]? Ret{get=>_ret;}  

    private RpcError? _error = null;
    public RpcError? Error{get=>_error;}

    public RpcResponseMessage(UInt64 seqno,byte[] ret)
    {
        _seqno = seqno;
        _ret = ret;
    }

    public RpcResponseMessage(UInt64 seqno,RpcError err)
    {
        _seqno = seqno;
        _error = err;
    }


}



