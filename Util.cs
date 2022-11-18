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
    private Mutex mtx = new Mutex();

    public void Do(Action fn)
    {
        if(done)
        {
            return;
        }
        else 
        {
            using var guard = new LockGuard(mtx);
            if(!done)
            {
                fn();
                done = true;
            }
        }
    }
}