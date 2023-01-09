using System;
using System.Threading;
namespace ClusterDotNet;

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