using System;
using System.Threading;
namespace ClusterDotNet;

public class LockGuard : IDisposable
{
    private Mutex mtx;
    public LockGuard(Mutex mtx)
    {
        this.mtx = mtx;
        this.mtx.WaitOne();
    }

    public void Dispose()
    {
        this.mtx.ReleaseMutex();
    }
}

public class Once
{
    private bool done = false;
    private readonly object mtx = new object();

    public void Do(Action fn)
    {
        if(done)
        {
            return;
        }
        else 
        {
            lock(mtx)
            {
                if(!done)
                {
                    fn();
                    done = true;
                }                
            }
        }
    }
}