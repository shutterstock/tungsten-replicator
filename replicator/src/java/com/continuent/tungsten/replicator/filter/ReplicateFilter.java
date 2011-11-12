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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cache.IndexedLRUCache;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.TableMatcher;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a filter to either apply or ignore operations on particular
 * schemas and/or tables. Patterns are comma separated lists, where each entry
 * may have the following form:
 * <ul>
 * <li>A schema name, for example "test"</li>
 * <li>A fully qualified table name, for example "test.foo"</li>
 * </ul>
 * Schema and table names may contain * and ? characters, which substitute for a
 * series of characters or a single character, respectively. For example,
 * "test.*" matches all tables in database test, and "test?.foo" matches tables
 * "test1.foo" and "test2.foo" but not "test.foo".
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ReplicateFilter implements Filter
{
    private static Logger            logger = Logger.getLogger(ReplicateFilter.class);

    private TableMatcher             doMatcher;
    private TableMatcher             ignoreMatcher;

    private String                   doFilter;
    private String                   ignoreFilter;

    private String                   tungstenSchema;

    // Cache to look up filtered tables.
    private IndexedLRUCache<Boolean> filterCache;

    /**
     * Define a comma-separated list of schemas with optional table names (e.g.,
     * schema1,schema2.table1,etc.) to replicate. If set, only operations that
     * match the list will be forwarded.
     */
    public void setDoFilter(String doFilter)
    {
        this.setDo(doFilter);
    }

    public void setDo(String doFilter)
    {
        this.doFilter = doFilter;
    }

    /**
     * Define a comma-separated list of schemas with optional table names (e.g.,
     * schema1,schema2.table1,etc.) to ignore. If set, all operations that match
     * the list will be ignored.
     * 
     * @param ignoreFilter
     */
    public void setIgnoreFilter(String ignoreFilter)
    {
        setIgnore(ignoreFilter);
    }

    public void setIgnore(String ignore)
    {
        this.ignoreFilter = ignore;
    }

    /**
     * Filters transactions using do and ignore rules. The logic is as follows.
     * <ol>
     * <li>If the operation matches a schema or table to ignore, drop it.</li>
     * <li>If the operation matches a schema or table to do, forward it.</li>
     * <li>If the do list is enabled and the operation does not match, drop it.</li>
     * </ol>
     * Individual operations that match the filtering rules are removed. If the
     * entire transaction becomes empty as a result, it will be removed.
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        ArrayList<DBMSData> data = event.getData();

        if (data == null)
            return event;

        for (Iterator<DBMSData> iterator = data.iterator(); iterator.hasNext();)
        {
            DBMSData dataElem = iterator.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> iterator2 = rdata.getRowChanges()
                        .iterator(); iterator2.hasNext();)
                {
                    OneRowChange orc = iterator2.next();

                    if (filterEvent(orc.getSchemaName(), orc.getTableName()))
                    {
                        iterator2.remove();
                    }
                }
                if (rdata.getRowChanges().isEmpty())
                {
                    iterator.remove();
                }
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                String schema = null;
                String table = null;

                Object parsingMetadata = sdata.getParsingMetadata();
                if (parsingMetadata != null
                        && parsingMetadata instanceof SqlOperation)
                {
                    SqlOperation parsed = (SqlOperation) parsingMetadata;
                    schema = parsed.getSchema();
                    table = parsed.getName();
                    if (logger.isDebugEnabled())
                        logger.debug("Parsing found schema = " + schema
                                + " / table = " + table);
                }

                if (schema == null)
                    schema = sdata.getDefaultSchema();

                if (schema == null)
                {
                    final String query = sdata.getQuery();
                    logger.warn("Ignoring event : No schema found for this event "
                            + event.getSeqno()
                            + (query != null ? " ("
                                    + query.substring(0,
                                            Math.min(query.length(), 200))
                                    + "...)" : ""));
                    continue;
                }

                if (filterEvent(schema, table))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Filtering event");
                    iterator.remove();
                }
            }
        }

        if (data.isEmpty())
        {
            return null;
        }
        return event;
    }

    // Returns true if the schema and table should be filtered using either a
    // cache look-up or a full scan based on filtering rules.
    private boolean filterEvent(String schema, String table)
    {
        // if schema not provided, cannot filter
        if (schema.length() == 0)
            return false;

        // Find out if we need to filter.
        String key = fullyQualifiedName(schema, table);
        Boolean filter = filterCache.get(key);
        if (filter == null)
        {
            filter = filterEventRaw(schema, table);
            filterCache.put(key, filter);
        }

        // Return a value.
        return filter;
    }

    // Performs a scan of all rules to see if we need to filter this event.
    private boolean filterEventRaw(String schema, String table)
    {
        // Tungsten schema is always passed through as dropping this can
        // confuse the replicator.
        if (schema.equals(tungstenSchema))
            return false;

        // Check to see if we explicitly accept this schema/table.
        if (doMatcher != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Checking if we should replicate: schema="
                        + schema + " table=" + table);
            if (doMatcher.match(schema, table))
                return false;
        }

        // Now check to see if we explicitly ignore this schema/table.
        if (ignoreMatcher != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Checking if we should ignore: schema=" + schema
                        + " table=" + table);
            if (ignoreMatcher.match(schema, table))
                return true;
        }

        // At this point check whether the do filters were used or not : if they
        // were, then it means that the table/schema that was looked for did not
        // match any of the filters => drop the event. 
        return doMatcher != null;
    }

    // Returns the fully qualified schema and/or table name, which can be used
    // as a key.
    public String fullyQualifiedName(String schema, String table)
    {
        StringBuffer fqn = new StringBuffer();
        fqn.append(schema);
        if (table != null)
            fqn.append(".").append(table);
        return fqn.toString();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        tungstenSchema = context.getReplicatorProperties().getString(
                ReplicatorConf.METADATA_SCHEMA);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Preparing Replicate Filter");

        // Implement filter rules.
        this.doMatcher = extractFilter(doFilter);
        this.ignoreMatcher = extractFilter(ignoreFilter);

        // Initialize LRU cache.
        this.filterCache = new IndexedLRUCache<Boolean>(1000, null);
    }

    // Prepares table matcher.
    private TableMatcher extractFilter(String filter)
    {
        // If empty, we do nothing.
        if (filter == null || filter.length() == 0)
            return null;

        TableMatcher tableMatcher = new TableMatcher();
        tableMatcher.prepare(filter);
        return tableMatcher;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        this.filterCache.invalidateAll();
    }
}
