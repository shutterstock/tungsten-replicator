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

package com.continuent.tungsten.commons.jmx;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import javax.management.MBeanOperationInfo;

import com.continuent.tungsten.commons.utils.CLUtils;
import com.continuent.tungsten.commons.utils.ReflectUtils;

/**
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class DynamicMBeanOperation implements Serializable
{
    /**
     * 
     */
    private static final long              serialVersionUID = 1L;
    private String                         name             = null;
    private String                         description      = null;
    private String                         usage            = null;
    private Vector<String>                 signature        = new Vector<String>();
    private Map<String, DynamicMBeanParam> params           = new LinkedHashMap<String, DynamicMBeanParam>();

    /**
     * @param method
     * @param info
     */
    public DynamicMBeanOperation(Method method, MBeanOperationInfo info)
    {
        name = method.getName();

        MethodDesc mdesc = (MethodDesc) method.getAnnotation(MethodDesc.class);
        if (mdesc != null)
        {
            if (!mdesc.description().equals(""))
                description = mdesc.description();
            else
                description = name;

            if (!mdesc.usage().equals(""))
                usage = mdesc.usage();
        }

        setParamsAndSignature(method, info);
    }

    /**
     * TODO: setParamsAndSignature definition.
     * 
     * @param method
     * @param info
     */
    private void setParamsAndSignature(Method method, MBeanOperationInfo info)
    {
        Class<?>[] paramTypes = method.getParameterTypes();
        Annotation[][] paramAnnotations = method.getParameterAnnotations();

        for (int i = 0; i < paramTypes.length; i++)
        {
            String name = String.format("param%d", i + 1);
            String description = name;
            String displayName = name;

            Class<?> type = paramTypes[i];

            for (Annotation annotation : paramAnnotations[i])
            {
                if (annotation instanceof ParamDesc)
                {
                    ParamDesc desc = (ParamDesc) annotation;
                    name = desc.name();
                    if (!desc.description().equals(""))
                    {
                        description = desc.description();
                    }
                    else
                    {
                        description = name;
                    }
                    if (!desc.displayName().equals(""))
                    {
                        displayName = desc.displayName();
                    }
                    else
                    {
                        displayName = name;
                    }
                }
            }

            DynamicMBeanParam param = new DynamicMBeanParam(name, i,
                    displayName, description, type);

            // We put them in the map by display name
            params.put(displayName, param);
            signature.add(type.getName());

        }
    }

    /**
     * @return the signature of this operation
     */
    public String[] getSignature()
    {
        String[] signatureArray = new String[signature.size()];

        for (int i = 0; i < signature.size(); i++)
            signatureArray[i] = signature.get(i);

        return signatureArray;

    }

    /**
     * This method converts a map of named parameters into a position-ordered
     * set of parameters. At the same time that it is doing this, it verifies
     * that all parameters have been supplied, and that the types are correct.
     * 
     * @param paramMap
     * @return an array of objects in param order
     */
    @SuppressWarnings("unchecked")
    public Object[] validateAndGetNamedParams(Map<String, Object> paramMap)
            throws Exception
    {
        if (paramMap == null)
        {
            if (params.size() > 0)
            {
                throw new Exception(
                        String
                                .format(
                                        "No parameters passed ivalidateAndGetParamsn but %d were required",
                                        params.size()));
            }
        }

        if (params.size() == 0)
        {
            return null;
        }

        Object[] mbeanParams = new Object[params.size()];

        Map<String, DynamicMBeanParam> copyParams = new LinkedHashMap<String, DynamicMBeanParam>(
                params);

        for (String paramName : paramMap.keySet())
        {
            Object paramValue = paramMap.get(paramName);

            DynamicMBeanParam mbeanParam = copyParams.get(paramName);

            if (mbeanParam == null)
            {
                throw new Exception(String
                        .format("No parameter %s found for method %s.",
                                paramName, name));
            }

            // We have a parameter match, now check the datatype
            // match.
            if (paramValue != null)
            {
                if (paramValue.getClass() != mbeanParam.getType())
                {
                    try
                    {
                        // Determine whether we can do some parsing, so the
                        // later JMX call would not throw an invalid cast
                        // exception.
                        if (mbeanParam.getType().isPrimitive())
                        {
                            Class wrapperClass = ReflectUtils
                                    .primitiveToWrapper(mbeanParam.getType());
                            paramValue = wrapperClass.getConstructor(
                                    String.class).newInstance(paramValue);
                        }
                    }
                    catch (Exception e)
                    {
                        // Ignore any exception as this is a usability
                        // increasing step. We pass the argument as is in case
                        // of exception.
                    }
                }
            }

            mbeanParams[mbeanParam.getOrder()] = paramValue;
            copyParams.remove(paramName);
        }

        // Finally, check to make sure all required params were supplied.
        if (!copyParams.isEmpty())
        {
            throw new Exception(
                    String
                            .format(
                                    "Parameters passed in were missing required parameters.\n%s%s\nUsage: %s",
                                    formatParams(copyParams, true), usage()));
        }

        return mbeanParams;

    }

    public Object[] validateAndGetPositionalParams(Map<String, Object> paramMap)
            throws Exception
    {
        if (paramMap == null)
        {
            if (params.size() > 0)
            {
                throw new Exception(String.format(
                        "No parameters passed but %d were required", params
                                .size()));
            }
        }

        if (params.size() == 0)
        {
            return null;
        }

        Object[] mbeanParams = new Object[params.size()];
        Object[] copyMBeanParams = (Object[]) (params.values().toArray());
        int i = 0;
        for (Object param : paramMap.values())
        {
            // Determine whether we can do some parsing, so the
            // later JMX call would not throw an invalid cast
            // exception.
            try
            {
                DynamicMBeanParam mbeanParam = (DynamicMBeanParam) copyMBeanParams[i];
                if (param != null)
                {
                    // Parse null.
                    if (param.getClass() == String.class
                            && param.toString().compareTo("null") == 0)
                    {
                        param = null;
                    }
                    // Parse primitive types.
                    else if (param.getClass() != mbeanParam.getType())
                    {
                        if (mbeanParam.getType().isPrimitive())
                        {
                            Class<?> wrapperClass = ReflectUtils
                                    .primitiveToWrapper(mbeanParam.getType());
                            param = wrapperClass.getConstructor(String.class)
                                    .newInstance(param);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                // Ignore any exception as this is a usability
                // increasing step. We pass the argument as is in case
                // of exception.
            }

            mbeanParams[i++] = param;
        }

        return mbeanParams;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        if (params.size() > 0 && false)
        {
            builder.append(String.format("%s(%s)", name, formatParams(params,
                    false)));
        }
        else
        {
            builder.append(String.format("%s", name));
        }
        return builder.toString();
    }

    /**
     * Single line representation of parameters.
     */
    private String formatParams(Map<String, DynamicMBeanParam> params,
            boolean includeDescription)
    {
        return formatParams(params, includeDescription, false, null);
    }

    /**
     * @param multiLine Format parameters to multiple lines? More tidy but
     *            consumes more text space.
     * @param linePrefix String to use before every line. Used with
     *            multiLine=true.
     */
    private String formatParams(Map<String, DynamicMBeanParam> params,
            boolean includeDescription, boolean multiLine, String linePrefix)
    {
        StringBuilder builder = new StringBuilder();

        for (DynamicMBeanParam param : params.values())
        {
            if (builder.length() > 0)
            {
                if (multiLine)
                    builder.append("\n");
                else
                    builder.append(", ");
            }

            if (includeDescription)
            {
                if (multiLine)
                {
                    if (linePrefix != null)
                        builder.append(linePrefix);
                    builder.append(String.format("%-25s : %s", param, param
                            .getDescription()));
                }
                else
                    builder.append(String.format("%20s : %s", param, param
                            .getDescription()));
            }
            else
            {
                builder.append(String.format("%s", param));
            }
        }

        return builder.toString();
    }

    /**
     * TODO: usage definition.
     * 
     * @return A string representing the usage of this operation
     */
    public String usage()
    {
        StringBuilder builder = new StringBuilder();

        if (usage != null)
            builder.append(String.format("Usage: %s\n", usage));

        builder.append(String
                .format("%s(%s)", name, formatParams(params, true)));

        return builder.toString();
    }

    public String getParamDescription(boolean multiLine, String linePrefix)
    {
        return formatParams(params, true, multiLine, linePrefix);
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
     * Returns the usage value.
     * 
     * @return Returns the usage.
     */
    public String getUsage()
    {
        return usage;
    }

    /**
     * Sets the usage value.
     * 
     * @param usage The usage to set.
     */
    public void setUsage(String usage)
    {
        this.usage = usage;
    }

    /**
     * Sets the signature value.
     * 
     * @param signature The signature to set.
     */
    public void setSignature(Vector<String> signature)
    {
        this.signature = signature;
    }

}
