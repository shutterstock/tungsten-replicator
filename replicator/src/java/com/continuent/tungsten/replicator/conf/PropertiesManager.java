/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Provides consolidated handling of properties in replicator. Properties
 * consist of static properties read only from replicator.properties and dynamic
 * properties which are settable from client calls and stored in
 * dynamic.properties. Dynamic properties, if set, take precedence over static
 * properties.
 * <p>
 * This class has synchronization required to ensure properties operations are
 * visible across threads and to prevent property value inconsistencies when
 * writing and reading properties at the same time.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PropertiesManager
{
    private static Logger      logger = Logger
                                              .getLogger(PropertiesManager.class);

    private TungstenProperties staticProperties;
    private TungstenProperties dynamicProperties;

    // Locations of property files.
    private final File         staticPropertiesFile;
    private final File         dynamicPropertiesFile;

    /**
     * Creates a new <code>PropertiesManager</code> object
     * 
     * @param staticPropertiesFile File containing static properties
     *            (replicator.properties), which must exist
     * @param dynamicPropertiesFile File containing dynamic properties, which
     *            does not have to exist
     */
    public PropertiesManager(File staticPropertiesFile,
            File dynamicPropertiesFile)
    {
        this.staticPropertiesFile = staticPropertiesFile;
        this.dynamicPropertiesFile = dynamicPropertiesFile;
    }

    // Loads all properties. 
    public void loadProperties() throws ReplicatorException
    {
        loadStaticProperties();
        loadDynamicProperties();
    }

    /**
     * Returns current state of properties. Dynamic properties are automatically
     * read from files and merged to their current values.  You must load
     * properties before calling this method. 
     */
    public synchronized TungstenProperties getProperties()
    {
        TungstenProperties rawProps = new TungstenProperties();
        rawProps.putAll(staticProperties);
        rawProps.putAll(dynamicProperties);
        
        // Kludge to perform variable substitutions on merged properties. 
        // Otherwise we lose substitutions that come from the dynamic 
        // properties 
        Properties substitutedProps = rawProps.getProperties();
        TungstenProperties.substituteSystemValues(substitutedProps, 10);
        TungstenProperties props = new TungstenProperties();
        props.load(substitutedProps);
        return props;
    }

    /**
     * Clear in-memory dynamic properties and delete on-disk file, if it exists.
     */
    public synchronized void clearDynamicProperties() throws ReplicatorException
    {
        logger.info("Clearing dynamic properties");
        // Check for null; this may be invoked before properties are loaded. 
        if (dynamicProperties != null)
            dynamicProperties.clear();
        if (dynamicPropertiesFile.exists())
        {
            if (!dynamicPropertiesFile.delete())
                logger.error("Unable to delete dynamic properties file: "
                        + dynamicPropertiesFile.getAbsolutePath());
        }
    }

    /**
     * Return current values of all supported dynamic values. 
     */
    public synchronized TungstenProperties getDynamicProperties()
            throws ReplicatorException
    {
        validateProperties();
        TungstenProperties dynamic = new TungstenProperties();
        TungstenProperties all = getProperties();
        for (String dynamicName: ReplicatorConf.DYNAMIC_PROPERTIES)
        {
            dynamic.setString(dynamicName, all.getString(dynamicName));
        }
        return dynamic; 
    }

    /**
     * Sets one or more dynammic properties after checking we permit them to be 
     * set. 
     */
    public synchronized void setDynamicProperties(TungstenProperties dynaProps)
            throws ReplicatorException
    {
        validateProperties();

        // Ensure each property may be set dynamically.
        for (String name: dynaProps.keyNames())
        {
            boolean settable = false;
            for (String settableName : ReplicatorConf.DYNAMIC_PROPERTIES)
            {
                if (settableName.equals(name))
                {
                    settable = true;
                    break;
                }
            }
            if (!settable)
                throw new ReplicatorException(
                        "Property does not exist or is not dynamically settable: "
                                + name);
        }

        // Update dynamic properties.
        dynamicProperties.putAll(dynaProps);
        FileOutputStream fos = null;
        try
        {
            fos = new FileOutputStream(dynamicPropertiesFile);
            dynamicProperties.store(fos);
        }
        catch (IOException e)
        {
            String msg = "Unable to write dymamic properties file: "
                    + dynamicPropertiesFile.getAbsolutePath();
            throw new ReplicatorException(msg, e);
        }
        finally
        {
            if (fos != null)
            {
                try
                {
                    fos.close();
                }
                catch (IOException e)
                {
                }
            }
        }

        // List updated properties. 
        for (String name: dynaProps.keyNames())
        {
            logger.info("Dynamic property updated: name=" + name + " value="
                    + dynaProps.getString(name));
        }
    }

    // Ensure properties are loaded.
    private void validateProperties() throws ReplicatorException
    {
        if (staticProperties == null || dynamicProperties == null)
        {
            loadProperties();
        }
    }

    /**
     * Load static properties from current replicator.properties location.
     */
    private void loadStaticProperties() throws ReplicatorException
    {
        logger.debug("Reading static properties file: "
                + staticPropertiesFile.getAbsolutePath());
        staticProperties = loadProperties(staticPropertiesFile);
    }

    /**
     * Load dynamic properties from current dynamic.properties location. If the
     * properties file does not exist, we make the properties empty.
     */
    private void loadDynamicProperties() throws ReplicatorException
    {
        if (dynamicPropertiesFile.exists())
        {
            logger.debug("Reading dynamic properties file: "
                    + dynamicPropertiesFile.getAbsolutePath());
            dynamicProperties = loadProperties(dynamicPropertiesFile);
        }
        else
            dynamicProperties = new TungstenProperties();
    }

    // Loads a properties file throwing an exception if there is a failure.
    public static TungstenProperties loadProperties(File propsFile)
            throws ReplicatorException
    {
        try
        {
            TungstenProperties newProps = new TungstenProperties();
            newProps.load(new FileInputStream(propsFile), false);
            return newProps;
        }
        catch (FileNotFoundException e)
        {
            logger.error("Unable to find properties file: " + propsFile);
            logger.debug("Properties search failure", e);
            throw new ReplicatorException("Unable to find properties file: "
                    + e.getMessage());
        }
        catch (IOException e)
        {
            logger.error("Unable to read properties file: " + propsFile);
            logger.debug("Properties read failure", e);
            throw new ReplicatorException("Unable to read properties file: "
                    + e.getMessage());
        }
    }
}
