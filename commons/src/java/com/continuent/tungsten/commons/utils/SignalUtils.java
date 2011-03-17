
package com.continuent.tungsten.commons.utils;

import java.lang.reflect.Method;

import sun.misc.Signal;
import sun.misc.SignalHandler;

// Application Wrapper
// usage: java AppWrap <app name> <app arg1> ... <app argn>
// where: <app name> is the name of the wrapped application class
//                   containing a main method
//        <app arg1> ... <app argn> are the application's arguments
public class SignalUtils
{
    public static void main(String[] args)
    {
        try
        {
            // Install diagnostic signal handler
            DiagSignalHandler.install("SIGINT");

            // Get the passed application's class
            Class<?> wrappedClass = Class.forName(args[0]);

            // Setup application's input arguments
            String wrappedArgs[] = new String[args.length - 1];
            for (int i = 0; i < wrappedArgs.length; i++)
            {
                wrappedArgs[i] = args[i + 1];
            }

            // Get the main method for the application
            Class<?>[] argTypes = new Class[1];
            argTypes[0] = wrappedArgs.getClass();
            Method mainMethod = wrappedClass.getMethod("main", argTypes);

            // Invoke the application's main method
            Object[] argValues = new Object[1];
            argValues[0] = wrappedArgs;
            mainMethod.invoke(wrappedClass, argValues);

        }
        catch (Exception e)
        {
            System.out.println("AppWrap exception " + e);
        }
    }
}
// Diagnostic Signal Handler class definition
class DiagSignalHandler implements SignalHandler
{

    private SignalHandler oldHandler;

    // Static method to install the signal handler
    public static DiagSignalHandler install(String signalName)
    {
        Signal diagSignal = new Signal(signalName);
        DiagSignalHandler diagHandler = new DiagSignalHandler();
        diagHandler.oldHandler = Signal.handle(diagSignal, diagHandler);
        return diagHandler;
    }

    // Signal handler method
    public void handle(Signal sig)
    {
        System.out
                .println("Diagnostic Signal handler called for signal " + sig);
        
        if (true)
            return;
        
        try
        {
            // Output information for each thread
            Thread[] threadArray = new Thread[Thread.activeCount()];
            int numThreads = Thread.enumerate(threadArray);
            System.out.println("Current threads:");
            for (int i = 0; i < numThreads; i++)
            {
                System.out.println("    " + threadArray[i]);
            }
            
            
            // Chain back to previous handler, if one exists
            if (oldHandler != SIG_DFL && oldHandler != SIG_IGN)
            {
                oldHandler.handle(sig);
            }

        }
        catch (Exception e)
        {
            System.out.println("Signal handler failed, reason " + e);
        }
    }
}
