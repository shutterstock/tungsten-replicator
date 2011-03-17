
package com.continuent.tungsten.commons.cluster.resource;

import java.io.Serializable;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.utils.ReflectUtils;
import com.continuent.tungsten.commons.utils.ResultFormatter;
import com.continuent.tungsten.commons.directory.DirectoryType;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

/**
 * @author edward
 */
@org.jboss.cache.pojo.annotation.Replicable
@XStreamAlias("Resource")
public abstract class Resource implements Serializable
{
    /**
     * 
     */
    private static final long    serialVersionUID = 1L;

    public static final String   NAME             = "name";
    public static final String   TYPE             = "type";
    public static final String   STATE            = "state";
    public static final String   DESCRIPTION      = "description";

    protected String             name             = null;
    protected ResourceType       type             = ResourceType.UNDEFINED;
    protected TungstenProperties properties       = new TungstenProperties();

    @XStreamOmitField
    protected ResourceState      state            = ResourceState.UNKNOWN;
    protected String             description      = "";
    @XStreamOmitField
    protected boolean            isContainer      = false;
    @XStreamOmitField
    protected boolean            isExecutable     = false;
    @XStreamOmitField
    protected ResourceType       childType        = ResourceType.UNDEFINED;
    @XStreamOmitField
    protected DirectoryType      directoryType    = DirectoryType.UNDEFINED;

    public Resource()
    {
        this.name = "UNKNOWN";
    }

    public Resource(ResourceType type, String name)
    {
        this.name = name;
        this.type = type;
    }

    public TungstenProperties toProperties()
    {
        return getProperties();
    }

    /**
     * Describe this instance, in detail if necessary.
     * 
     * @param detailed
     * @return string description of this resource
     */
    public String describe(boolean detailed)
    {
        TungstenProperties props = this.toProperties();
        String formattedProperties = (new ResultFormatter(props.map(), false,
                ResultFormatter.DEFAULT_INDENT)).format();
        return (String.format("%s\n{\n%s\n}", name, formattedProperties));
    }

    public String toString()
    {
        String ret = getName();

        if (isContainer())
            ret += "/";

        return ret;
    }

    /**
     * 
     */
    public String getKey()
    {
        return getName();
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }
    
    public String getFqn()
    {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * @return the type
     */
    public ResourceType getType()
    {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(ResourceType type)
    {
        this.type = type;
    }

    /**
     * @return the isContainerResource
     */
    public boolean isContainer()
    {
        return isContainer;
    }

    /**
     * @param isContainer the isContainer to set
     */
    public void setContainer(boolean isContainer)
    {
        this.isContainer = isContainer;
    }

    /**
     * @return the isExecutable
     */
    public boolean isExecutable()
    {
        return isExecutable;
    }

    /**
     * @param isExecutable the isExecutable to set
     */
    public void setExecutable(boolean isExecutable)
    {
        this.isExecutable = isExecutable;
    }

    /**
     * @return the childType
     */
    public ResourceType getChildType()
    {
        return childType;
    }

    /**
     * @param childType the childType to set
     */
    public void setChildType(ResourceType childType)
    {
        this.childType = childType;
    }

    /**
     * Copies values from fields of this instance to another instance
     * 
     * @param destination
     * @return the copied resource
     */
    public Resource copyTo(Resource destination)
    {
        ReflectUtils.copy(this, destination);
        destination.setName(this.getName());
        return destination;
    }

    /**
     * Returns the state value.
     * 
     * @return Returns the state.
     */
    public ResourceState getState()
    {
        return state;
    }

    /**
     * Sets the state value.
     * 
     * @param state The state to set.
     */
    public void setState(ResourceState state)
    {
        this.state = state;
    }

    /**
     * Returns the directoryType value.
     * 
     * @return Returns the directoryType.
     */
    public DirectoryType getDirectoryType()
    {
        return directoryType;
    }

    /**
     * Sets the directoryType value.
     * 
     * @param directoryType The directoryType to set.
     */
    public void setDirectoryType(DirectoryType directoryType)
    {
        this.directoryType = directoryType;
    }

    /**
     * Returns the properties value.
     * 
     * @return Returns the properties.
     */
    public TungstenProperties getProperties()
    {
        return properties;
    }

    /**
     * Sets the properties value.
     * 
     * @param properties The properties to set.
     */
    public void setProperties(TungstenProperties properties)
    {
        this.properties = properties;
    }

    public void setProperty(String key, Object value)
    {
        properties.setObject(key, value);
    }

    public Object getProperty(String key)
    {
        return properties.getObject(key);
    }

}
