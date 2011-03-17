
package com.continuent.tungsten.commons.cluster.resource.physical;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.directory.DirectoryType;

public class SQLRouter extends Resource
{

    private static final long serialVersionUID = 8153881753668230575L;
    private int               port             = 0;
    private String            host             = null;
    private String            role             = null;
    // the source for the THL for this replicator - valid if it is a slave.
    private String            source           = null;
    private String            dataServiceName  = null;
    private DirectoryType     directoryType    = DirectoryType.PHYSICAL;

    public SQLRouter(TungstenProperties props)
    {
        super(ResourceType.SQLROUTER, props.getString("name", "router", true));
        props.applyProperties(this, true);
    }

    /**
     * @return the port
     */
    public int getPort()
    {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(int port)
    {
        this.port = port;
    }

    /**
     * @return the host
     */
    public String getHost()
    {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * @return the role
     */
    public String getRole()
    {
        return role;
    }

    /**
     * @param role the role to set
     */
    public void setRole(String role)
    {
        this.role = role;
    }

    /**
     * @return the source
     */
    public String getSource()
    {
        return source;
    }

    /**
     * @param source the source to set
     */
    public void setSource(String source)
    {
        this.source = source;
    }

    public String getDataServiceName()
    {
        return dataServiceName;
    }

    public void setDataServiceName(String dataServiceName)
    {
        this.dataServiceName = dataServiceName;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return describe(false);
    }

}
