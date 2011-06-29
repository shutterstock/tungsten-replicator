/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.storage.parallel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Utility functions for partitioning operations.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PartitionerUtility
{
    private static Logger logger = Logger.getLogger(PartitionerUtility.class);

    /**
     * Find and load shard properties.
     * 
     * @param shardMap File containing shard properties
     */
    public static TungstenProperties loadShardProperties(File shardMap)
            throws ReplicatorException
    {
        // Locate shard map file.
        if (shardMap == null)
        {
            shardMap = new File(System.getProperty("replicator.home.dir")
                    + File.separatorChar + "conf" + File.separatorChar
                    + "shard.list");
        }
        if (!shardMap.isFile() || !shardMap.canRead())
        {
            throw new ReplicatorException(
                    "Shard map file missing or unreadable: "
                            + shardMap.getAbsolutePath());
        }
        logger.info("Loading shard map file: " + shardMap.getAbsolutePath());

        // Load properties from the file.
        FileInputStream fis = null;
        TungstenProperties shardMapProperties = null;
        try
        {
            fis = new FileInputStream(shardMap);
            shardMapProperties = new TungstenProperties();
            shardMapProperties.load(fis);
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to load shard map file: "
                    + shardMap.getAbsolutePath(), e);
        }
        finally
        {
            if (fis != null)
            {
                try
                {
                    fis.close();
                }
                catch (IOException e)
                {
                }
            }
        }

        return shardMapProperties;
    }
}