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

import com.continuent.tungsten.commons.jmx.ServerRuntimeException;

/**
 * This class defines configuration values that are set at runtime through
 * system properties and provides convenient access to the same.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplicatorRuntimeConf
{
    /** Path to replicator release */
    static public final String HOME_DIR                 = "replicator.home.dir";
    /** Path to replicator log directory */
    static public final String LOG_DIR                  = "replicator.log.dir";
    /** Path to replicator conf directory */
    static public final String CONF_DIR                 = "replicator.conf.dir";
    /** Option to clear dynamic properties */
    static public final String CLEAR_DYNAMIC_PROPERTIES = "replicator.clear.dynamic.proeprties";

    // Static variables.
    private static File        replicatorHomeDir;
    private static File        replicatorLogDir;
    private static File        replicatorConfDir;

    private final File         replicatorProperties;
    private final File         replicatorDynamicProperties;
    private final boolean      clearDynamicProperties;

    /** Creates a new instance. */
    private ReplicatorRuntimeConf(String serviceName)
    {
        // Configure directory locations.
        replicatorHomeDir = locateReplicatorHomeDir();
        replicatorLogDir = locateReplicatorLogDir();
        replicatorConfDir = locateReplicatorConfDir();

        // Configure location of replicator.properties file.
        replicatorProperties = new File(locateReplicatorConfDir(), "static-"
                + serviceName + ".properties");

        if (!replicatorProperties.isFile() || !replicatorProperties.canRead())
        {
            throw new ServerRuntimeException(
                    "Replicator static properties does not exist or is invalid: "
                            + replicatorProperties);
        }

        // Configure location of replicator dynamic properties file.
        replicatorDynamicProperties = new File(replicatorConfDir, "dynamic-"
                + serviceName + ".properties");

        // Determine whether we want to clear dynamic properties at start-up.
        this.clearDynamicProperties = Boolean.parseBoolean(System
                .getProperty(CLEAR_DYNAMIC_PROPERTIES));
    }

    /**
     * Returns a configured replication runtime or throws an exception if
     * configuration fails.
     */
    public static ReplicatorRuntimeConf getConfiguration(String serviceName)
    {
        return new ReplicatorRuntimeConf(serviceName);
    }

    public File getReplicatorHomeDir()
    {
        return replicatorHomeDir;
    }

    public File getReplicatorConfDir()
    {
        return replicatorConfDir;
    }

    public File getReplicatorLogDir()
    {
        return replicatorLogDir;
    }

    public File getReplicatorProperties()
    {
        return replicatorProperties;
    }

    public File getReplicatorDynamicProperties()
    {
        return replicatorDynamicProperties;
    }

    public boolean getClearDynamicProperties()
    {
        return clearDynamicProperties;
    }

    /**
     * Find and return the replicator home directory.
     */
    public static File locateReplicatorHomeDir()
    {
        if (replicatorHomeDir == null)
        {
            // Configure replicator home.
            String replicatorHome = System.getProperty(HOME_DIR);
            if (replicatorHome == null)
                replicatorHome = System.getProperty("user.dir");
            replicatorHomeDir = new File(replicatorHome);
            if (!replicatorHomeDir.isDirectory())
            {
                throw new ServerRuntimeException(
                        "Replicator home does not exist or is invalid: "
                                + replicatorHomeDir);
            }
        }
        return replicatorHomeDir;
    }

    /**
     * Find and return the replicator log directory.
     */
    public static File locateReplicatorLogDir()
    {
        if (replicatorLogDir == null)
        {
            // Configure replicator log directory.
            String replicatorLog = System.getProperty(LOG_DIR);
            if (replicatorLog == null)
                replicatorLogDir = new File(locateReplicatorHomeDir(), "log");
            else
                replicatorLogDir = new File(replicatorLog);
            if (!replicatorLogDir.isDirectory())
            {
                throw new ServerRuntimeException(
                        "Replicator log directory does not exist or is invalid: "
                                + replicatorLogDir);
            }
        }
        return replicatorLogDir;
    }

    /**
     * Locate and return the replicator conf director.
     */
    public static File locateReplicatorConfDir()
    {
        if (replicatorConfDir == null)
        {
            // Configure replicator conf directory.
            String replicatorConf = System.getProperty(CONF_DIR);
            if (replicatorConf == null)
                replicatorConfDir = new File(locateReplicatorHomeDir(), "conf");
            else
                replicatorConfDir = new File(replicatorConf);
            if (!replicatorConfDir.isDirectory())
            {
                throw new ServerRuntimeException(
                        "Replicator conf directory does not exist or is invalid: "
                                + replicatorConfDir);
            }
        }
        return replicatorConfDir;
    }
}
