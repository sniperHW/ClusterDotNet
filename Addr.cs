using System;
using System.Threading;
using System.Net;
namespace SanguoDotNet;


public class Addr 
{       
    public LogicAddr LogicAddr{get;set;}

    private string _netAddr;
    
    public string NetAddr
    {
        get
        {
            return Interlocked.Exchange(ref _netAddr,_netAddr);
        }
        set
        {
            if(value != null)
            {
                Interlocked.Exchange(ref _netAddr,value);
            }            
        }
    }

    public Addr(string logicAddr,string netAddr)
    {   
        LogicAddr = new(logicAddr);
        _netAddr = netAddr;
    }

    public IPEndPoint IPEndPoint()
    {
        var r = NetAddr.Split(':');
        if(r.Length != 2) {
            throw new SanguoException("invaild netaddr");
        }
        return new IPEndPoint(IPAddress.Parse(r[0]),Convert.ToInt32(r[1]));
    }

}

public class LogicAddr : IComparable
{
    public const uint ClusterMask = 0xFFFC0000; //高14
    public const uint TypeMask = 0x0003FC00;    //中8
    public const uint ServerMask = 0x000003FF;  //低10
    public const uint HarbarType = 255;
    private uint _addr; 
    public uint ToUint32() 
    {
        return _addr;
    }

    public uint Cluster() {
        return (_addr & ClusterMask) >> 18;
    }

    public uint Type(){
	    return (_addr & TypeMask) >> 10;
    }

    public uint Server(){
	    return _addr & ServerMask;
    }

    override public string ToString(){
        return $"{Cluster()}.{Type()}.{Server()}";
    }

    public int CompareTo(Object? obj) 
    {
        if(obj == null) return 1;
        
        var other = obj as LogicAddr;

        if(other is null) {
            throw new ArgumentException("Object is not a LogicAddr");
        } else {
            return _addr.CompareTo(_addr);
        }   
    }


    public override bool Equals(object? obj)
    {
        if(obj is null) return false;

        var other = obj as LogicAddr;

        if(other is null) return false;
        
        return _addr == other._addr;
    }

    public override int GetHashCode() 
    {
        return base.GetHashCode();
    }

    public LogicAddr(uint addr)
    {
        _addr = addr;
        if(Cluster() == 0 || Cluster() > (ClusterMask>>18))
        {
		    throw new ArgumentException("invaild logic addr:invaild cluster");
	    }

        if(Type() == 0 || Type() > ((TypeMask>>10))) 
        {
		    throw new ArgumentException("invaild logic addr:invaild type");
	    }

        if(Server() > ServerMask)
        {
            throw new ArgumentException("invaild logic addr:invaild server");
        }
    }

    public LogicAddr(string str) 
    {
        char[] charSeparators = new char[] { '.' };
        string[] result = str.Split(charSeparators);
        if(result.Length != 3) 
        {
            throw new ArgumentException("invaild logic addr format('n.n.n')");
        }

        uint cluster = (uint)Convert.ToInt32(result[0]);
        uint type = (uint)Convert.ToInt32(result[1]);     
        uint server = (uint)Convert.ToInt32(result[2]);

        if(cluster == 0 || cluster > (ClusterMask>>18))
        {
		    throw new ArgumentException("invaild logic addr:invaild cluster");
	    }

        if(type == 0 || type > ((TypeMask>>10))) 
        {
		    throw new ArgumentException("invaild logic addr:invaild type");
	    }

        if(server > ServerMask)
        {
            throw new ArgumentException("invaild logic addr:invaild server");
        }


        _addr = 0 | (type << 10) | (cluster << 18) | server;
    }
}

