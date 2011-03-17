
package com.continuent.tungsten.commons.config.cluster;

import java.lang.reflect.Field;

import com.continuent.tungsten.commons.config.PropertyException;
import com.continuent.tungsten.commons.config.TungstenProperties;

public class ConfigUtilities
{
    static public void setProperty(String name, String value, Object o,
            TungstenProperties mirror,
            boolean ignoreIfNotFound) throws PropertyException
    {
        Field field = findField(name, o);

        if (field == null)
        {
            if (ignoreIfNotFound == true)
                return;

            throw new PropertyException("Could not find field=" + name);
        }

        try
        {
            if (field.getType() == Integer.TYPE)
            {
                field.setInt(o, new Integer(value).intValue());
            }
            else if (field.getType() == Long.TYPE)
            {
                field.setLong(o, new Long(value).longValue());
            }
            else if (field.getType() == Boolean.TYPE)
            {
                field.setBoolean(o, new Boolean(value)
                        .booleanValue());
            }
            else if (field.getType() == String.class)
            {
                field.set(o, value);
            }
            else
            {
                throw new PropertyException(
                        "Unsupported property type for 'setProperty':"
                                + field.getType());
            }
        }
        catch (IllegalAccessException i)
        {
            throw new PropertyException("Exception while trying to set field="
                    + field.getName() + " of class=" + o.getClass().getSimpleName());
        }
        
        mirror.setObject(name, value);
    }

    static private Field findField(String name, Object o)
    {
        Field foundField = null;

        Field[] fields = o.getClass().getDeclaredFields();

        for (Field field : fields)
        {
            if (field.getName().equals(name))
            {
                field.setAccessible(true);
                foundField = field;
                break;
            }
        }

        return foundField;
    }

}
