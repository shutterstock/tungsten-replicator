/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Edward Archibald
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.commons.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class ReflectUtils
{

    final static String NEWLINE = "\n";

    public static String describe(Object object)
    {
        StringBuilder builder = new StringBuilder();
        Class<?> clazz = object.getClass();

        Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        Field[] fields = clazz.getDeclaredFields();
        Method[] methods = clazz.getDeclaredMethods();

        builder.append("Description for class: " + clazz.getName()).append(NEWLINE);
        builder.append(NEWLINE).append(NEWLINE);
        builder.append("Summary").append(NEWLINE);
        builder.append("-----------------------------------------").append(NEWLINE);
        builder.append("Constructors: " + (constructors.length)).append(NEWLINE);
        builder.append("Fields: " + (fields.length)).append(NEWLINE);
        builder.append("Methods: " + (methods.length)).append(NEWLINE);

        builder.append(NEWLINE).append(NEWLINE);
        builder.append(NEWLINE).append(NEWLINE);
        builder.append("Details").append(NEWLINE);
        builder.append("-----------------------------------------").append(NEWLINE);

        if (constructors.length > 0)
        {
            builder.append(NEWLINE);
            builder.append("Constructors:").append(NEWLINE);
            for (Constructor<?> constructor : constructors)
            {
                builder.append(constructor).append(NEWLINE);
            }
        }

        if (fields.length > 0)
        {
            builder.append(NEWLINE);
            builder.append("Fields:").append(NEWLINE);
            for (Field field : fields)
            {
                builder.append(field).append(NEWLINE);
            }
        }

        if (methods.length > 0)
        {
            builder.append(NEWLINE);
            builder.append("Methods:").append(NEWLINE);
            for (Method method : methods)
            {
                builder.append(method).append(NEWLINE);
            }
        }

        return builder.toString();
    }

    public static String describeValues(Object object)
    {
        StringBuilder builder = new StringBuilder();

        Class<?> clazz = object.getClass();

        Field[] fields = clazz.getDeclaredFields();

        if (fields.length > 0)
        {
            builder.append(NEWLINE).append(
                    "-----------------------------------------")
                    .append(NEWLINE);
            for (Field field : fields)
            {
                builder.append(field.getName());
                builder.append(" = ");
                try
                {
                    field.setAccessible(true);
                    builder.append(field.get(object)).append(NEWLINE);
                }
                catch (IllegalAccessException e)
                {
                    builder.append("(Exception Thrown: " + e + ")");
                }
            }
        }

        return builder.toString();
    }

    public static Object clone(Object o)
    {
        Object clone = null;

        try
        {
            clone = o.getClass().newInstance();
        }
        catch (InstantiationException e)
        {
            e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
        }

        // Walk up the superclass hierarchy
        for (Class<?> obj = o.getClass(); !obj.equals(Object.class); obj = obj
                .getSuperclass())
        {
            Field[] fields = obj.getDeclaredFields();
            for (int i = 0; i < fields.length; i++)
            {
                fields[i].setAccessible(true);
                try
                {
                    // for each class/superclass, copy all fields
                    // from this object to the clone
                    fields[i].set(clone, fields[i].get(o));
                }
                catch (IllegalArgumentException e)
                {
                }
                catch (IllegalAccessException e)
                {
                }
            }
        }
        return clone;
    }
    
    
    public static Object copy(Object source, Object destination)
    {
        // Walk up the superclass hierarchy
        for (Class<?> obj = source.getClass(); !obj.equals(Object.class); obj = obj
                .getSuperclass())
        {
            Field[] fields = obj.getDeclaredFields();
            for (int i = 0; i < fields.length; i++)
            {
                fields[i].setAccessible(true);
                try
                {
                    // for each class/superclass, copy all fields
                    // from this object to the clone
                    fields[i].set(destination, fields[i].get(source));
                }
                catch (IllegalArgumentException e)
                {
                }
                catch (IllegalAccessException e)
                {
                }
            }
        }
        return destination;
    }

    /**
     * Maps primitive types to their corresponding wrapper classes.
     */
    private static Map primitiveWrapperMap = new HashMap();
    static
    {
        primitiveWrapperMap.put(Boolean.TYPE, Boolean.class);
        primitiveWrapperMap.put(Byte.TYPE, Byte.class);
        primitiveWrapperMap.put(Character.TYPE, Character.class);
        primitiveWrapperMap.put(Short.TYPE, Short.class);
        primitiveWrapperMap.put(Integer.TYPE, Integer.class);
        primitiveWrapperMap.put(Long.TYPE, Long.class);
        primitiveWrapperMap.put(Double.TYPE, Double.class);
        primitiveWrapperMap.put(Float.TYPE, Float.class);
    }

    /**
     * <p>
     * Converts the specified primitive type to its corresponding wrapper class.
     * </p>
     * 
     * @param clazz the class to convert, may be null
     * @return the wrapper class for <code>cls</code> or <code>cls</code> if
     *         <code>cls</code> is not a primitive. <code>null</code> if null
     *         input.
     */
    public static Class primitiveToWrapper(Class clazz)
    {
        Class convertedClass = clazz;
        if (clazz != null && clazz.isPrimitive())
        {
            convertedClass = (Class) primitiveWrapperMap.get(clazz);
        }
        return convertedClass;
    }
}
