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
 * Initial developer(s): Robert Hodges
 * Contributor(s): Linas Virbalas
 */
package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to transform a specific database name to a new value using Java
 * regular expression rules.  This filter matches the schema name using the 
 * fromRegex expression and then does a replacement on the name using the
 * toRegex expression.  
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 * @see java.util.regex.Pattern
 * @see java.util.regex.Matcher
 */
public class DatabaseTransformFilter implements Filter
{
    private static Logger logger = Logger.getLogger(LoggingFilter.class);

    private String fromRegex;
    private String toRegex;

    Pattern pattern;
    Matcher matcher;
    
    /** Sets the regex used to match the database name. */
    public void setFromRegex(String fromRegex)
    {
        this.fromRegex = fromRegex;
    }

    /** Sets the corresponding regex to transform the name. */
    public void setToRegex(String toRegex)
    {
        this.toRegex = toRegex;
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData(); 
        for (DBMSData dataElem : data)
        {
        	if (dataElem instanceof RowChangeData)
        	{
        		RowChangeData rdata = (RowChangeData) dataElem;
        		for (OneRowChange orc : rdata.getRowChanges())
        		{
        			matcher.reset(orc.getSchemaName());
        			if (matcher.matches())
        			{
        				String oldSchema = orc.getSchemaName();
        				orc.setSchemaName(matcher.replaceAll(matcher.replaceAll(toRegex)));
        				if (logger.isDebugEnabled())
        					logger.debug("Filtered event schema name: old=" + oldSchema
        							+ " new=" + orc.getSchemaName());
        			}
        		}
        	}
        	else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                String schema = sdata.getDefaultSchema();
                if(schema == null)
                    continue;
                matcher.reset(schema);
                if (matcher.matches())
                {
                    String oldSchema = schema;
                    sdata.setDefaultSchema(matcher.replaceAll(matcher
                            .replaceAll(toRegex)));
                    if (logger.isDebugEnabled())
                        logger.debug("Filtered event schema name: old="
                                + oldSchema + " new="
                                + sdata.getDefaultSchema());
                }
            }
        }
        return event;
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (fromRegex == null)
            throw new ReplicatorException("fromRegex property must be set for regex filter to work");
        if (toRegex == null)
            throw new ReplicatorException("toRegex property must be set for regex filter to work");
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Compile the pattern used for matching. 
        try
        {
            pattern = Pattern.compile(fromRegex);
            matcher = pattern.matcher("");
        }
        catch (PatternSyntaxException e)
        {
            throw new ReplicatorException("Replicator fromRegex is invalid:  expression=" 
                    + fromRegex, e);
        }
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
    }
}
