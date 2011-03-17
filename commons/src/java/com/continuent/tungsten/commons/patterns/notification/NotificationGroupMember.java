package com.continuent.tungsten.commons.patterns.notification;

import java.io.Serializable;

public class NotificationGroupMember implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 2416032277684423671L;
    
    private String name;
    private String host;
    private int port;
    
    public NotificationGroupMember(String name, String host, int port)
    {
        this.name = name;
        this.host = host;
        this.port = port;
    }
    
    public String toString()
    {
        return String.format("%s(host=%s, port=%d)", name, host, port);
    }

    /**
     * Returns a name suitable for use in maps.
     * @return generalized name
     */
    public static String getMemberAddress(String name)
    {
       if (name != null)
       {
           int slashIndex = name.indexOf("/");
           int colonIndex = name.indexOf(":");
           
           if (slashIndex == -1 || colonIndex == -1)
               return name;
           
           return name.substring(slashIndex + 1, colonIndex);
           
       }
          
       
       return "UNKNOWN";
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }
}
