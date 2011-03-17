
package com.continuent.tungsten.commons.utils;

public class ExecutionResult
{
    private ExecutionStatus status        = ExecutionStatus.UNDEFINED;
    private String          message       = null;
    private Exception       lastException = null;

    public ExecutionResult(ExecutionStatus status, Exception lastException,
            String message)
    {
        this.status = status;
        this.lastException = lastException;
        this.message = message;
    }

    public ExecutionResult(ExecutionStatus status)
    {
        this.status = status;
        this.lastException = null;
        this.message = null;
    }

    /**
     * Returns the message value.
     * 
     * @return Returns the message.
     */
    public synchronized String getMessage()
    {
        return message;
    }

    /**
     * Sets the message value.
     * 
     * @param message The message to set.
     */
    public synchronized void setMessage(String message)
    {
        this.message = message;
    }

    /**
     * Returns the lastException value.
     * 
     * @return Returns the lastException.
     */
    public synchronized Exception getLastException()
    {
        return lastException;
    }

    /**
     * Sets the lastException value.
     * 
     * @param lastException The lastException to set.
     */
    public synchronized void setLastException(Exception lastException)
    {
        this.lastException = lastException;
    }

    /**
     * Returns the status value.
     * 
     * @return Returns the status.
     */
    public synchronized ExecutionStatus getStatus()
    {
        return status;
    }

    /**
     * Sets the status value.
     * 
     * @param status The status to set.
     */
    public synchronized void setStatus(ExecutionStatus status)
    {
        this.status = status;
    }

    public String toString()
    {
        String ret = "status=" + status + ", message=" + message;

        if (lastException != null)
        {
            ret += ", exception=" + lastException;
        }

        return ret;
    }

}
