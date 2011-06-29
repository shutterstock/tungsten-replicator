/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.event;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Tracks number of schemas in a transaction.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventSchemaStatistics
{
    private static Logger        logger             = Logger.getLogger(EventMetadataFilter.class);

    // Map relating schema names and references.
    private Map<String, Integer> dbMap              = new HashMap<String, Integer>();

    // Counts.
    private int                  normalDbCount      = 0;
    private int                  tungstenDbCount    = 0;
    private String               singleDbName       = null;
    private String               service            = null;
    private String               lastDb             = null;

    // Pattern for tungsten schema name.
    private static Pattern       serviceNamePattern = Pattern
                                                            .compile(
                                                                    "tungsten_([a-zA-Z0-9-_]+)",
                                                                    Pattern.CASE_INSENSITIVE);

    public EventSchemaStatistics()
    {
    }

    public Map<String, Integer> getDbMap()
    {
        return dbMap;
    }

    public int getNormalDbCount()
    {
        return normalDbCount;
    }

    public int getTungstenDbCount()
    {
        return tungstenDbCount;
    }

    public String getSingleDbName()
    {
        return singleDbName;
    }

    public String getService()
    {
        return service;
    }

    /** Add a schema name, thereby incrementing counts. */
    void incrementSchema(String schemaName)
    {
        if (schemaName != null)
        {
            Integer count = dbMap.get(schemaName);
            if (count == null)
                dbMap.put(schemaName, 1);
            else
                dbMap.put(schemaName, count.intValue() + 1);
        }
        lastDb = schemaName;
    }

    /**
     * Count the schemas. This must be invoked before accessing the counts.
     */
    public void countSchemas()
    {
        for (String schemaName : dbMap.keySet())
        {
            // Tungsten database names define the service name in a transaction.
            String nextServiceName = schemaToServiceName(schemaName);
            if (nextServiceName == null)
            {
                // This is a normal database.
                singleDbName = schemaName;
                normalDbCount++;
                if (logger.isDebugEnabled())
                    logger.debug("Found local database: " + schemaName);
            }
            else
            {
                // We have a tungsten database, which is the service name.
                tungstenDbCount++;
                singleDbName = schemaName;
                service = nextServiceName;
                if (logger.isDebugEnabled())
                    logger.debug("Found tungsten database: " + schemaName);
            }
        }

        // Special case: if there multiple Tungsten databases, the service
        // comes from the last schema entered.
        if (tungstenDbCount > 1)
        {
            // This may be null if the last name is not a Tungsten schema.
            service = schemaToServiceName(lastDb);
        }
    }

    // Utility routine to returns the service name if this is a tungsten
    // metadata schema or null if it is an ordinary schema.
    private String schemaToServiceName(String schema)
    {
        Matcher m = serviceNamePattern.matcher(schema);
        if (m.find())
            return m.group(1);
        else
            return null;
    }

    // Return a nice set of counts for diagnostic purposes.
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        for (String schema : dbMap.keySet())
        {
            if (sb.length() > 1)
                sb.append(";");
            sb.append(schema).append("=>").append(dbMap.get(schema));
        }
        sb.append("}");
        return sb.toString();
    }
}
