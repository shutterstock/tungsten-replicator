
package com.continuent.tungsten.commons.jmx;

import java.io.Serializable;

/**
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 *
 */
public class DynamicMBeanParam implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private String   name        = null;
    private String   displayName = null;
    private String   description = null;
    int              order       = -1;
    private Class<?> type        = null;

   /**
    * 
    * @param name
    * @param order
    * @param displayName
    * @param description
    * @param type
    */
    public DynamicMBeanParam(String name, int order, String displayName,
            String description, Class<?> type)
    {
        this.name = name;
        this.order = order;
        
        if (displayName == null)
            this.displayName = name;
        else
            this.displayName = displayName;
        
        if (description == null)
            this.description = name;
        else
            this.description = description;
        
        this.type = type;
    }

    /**
     * Returns the name value.
     * 
     * @return Returns the name.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name value.
     * 
     * @param name The name to set.
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Returns the displayName value.
     * 
     * @return Returns the displayName.
     */
    public String getDisplayName()
    {
        return displayName;
    }

    /**
     * Sets the displayName value.
     * 
     * @param displayName The displayName to set.
     */
    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
    }

    /**
     * Returns the description value.
     * 
     * @return Returns the description.
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * Sets the description value.
     * 
     * @param description The description to set.
     */
    public void setDescription(String description)
    {
        this.description = description;
    }

    /**
     * Returns the type value.
     * 
     * @return Returns the type.
     */
    public Class<?> getType()
    {
        return type;
    }

    /**
     * Sets the type value.
     * 
     * @param type The type to set.
     */
    public void setType(Class<?> type)
    {
        this.type = type;
    }

    /**
     * Returns the order value.
     * 
     * @return Returns the order.
     */
    public int getOrder()
    {
        return order;
    }

    /**
     * Sets the order value.
     * 
     * @param order The order to set.
     */
    public void setOrder(int order)
    {
        this.order = order;
    }

    
    
    @Override
    public String toString()
    {
        return (String.format("%s %s", classNameOnly(type.getName()), displayName));
    }
    
    /**
     * 
     * @param className
     * @return
     */
    private String classNameOnly(String className)
    {
        return className.substring(className.lastIndexOf('.') + 1);
    }

}
