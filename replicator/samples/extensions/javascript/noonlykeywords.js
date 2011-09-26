/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * JavaScript example for JavaScriptFilter.
 *
 * Translates DELETE FROM ONLY -> DELETE FROM and UPDATE ONLY -> UPDATE.
 * Use case example: on a slave of PostgreSQL -> MySQL replication.
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.noonlykeywords=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.noonlykeywords.script=${replicator.home.dir}/samples/extensions/javascript/noonlykeywords.js
 *
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */
 
/**
 * Called on every filtered event. See replicator's javadoc for more details
 * on accessible classes. Also, JavaScriptFilter's javadoc contains description
 * about how to define a script like this.
 *
 * @param event Filtered com.continuent.tungsten.replicator.event.ReplDBMSEvent
 *
 * @see com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * @see com.continuent.tungsten.replicator.event.ReplDBMSEvent
 * @see com.continuent.tungsten.replicator.dbms.DBMSData
 * @see com.continuent.tungsten.replicator.dbms.StatementData
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData
 * @see com.continuent.tungsten.replicator.dbms.OneRowChange
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType
 * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#printRowChangeData(StringBuilder, RowChangeData, String, boolean, int)
 * @see java.lang.Thread
 * @see org.apache.log4j.Logger
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    logger.info("noonlykeywords: will translate DELETE FROM ONLY -> DELETE FROM and UPDATE ONLY -> UPDATE statements");
}

function filter(event)
{
    // Analyse what this event is holding.
    data = event.getData();
    
    // One ReplDBMSEvent may contain many DBMSData events.
    for(i=0;i<data.size();i++)
    {
        // Get com.continuent.tungsten.replicator.dbms.DBMSData
        d = data.get(i);
    
        // Determine the underlying type of DBMSData event.
        if(d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
        {
            // It's a SQL statement event - search & remove comments.
            sqlOriginal = d.getQuery();
            if(sqlOriginal.toUpperCase().startsWith("DELETE FROM ONLY") ||
               sqlOriginal.toUpperCase().startsWith("UPDATE ONLY"))
            {
                sqlNew = sqlOriginal.replaceFirst(" (?i)ONLY", "");
                d.setQuery(sqlNew);
                if(logger.isDebugEnabled())
                    logger.debug("noonlykeywords: " + sqlOriginal + " -> " + d.getQuery());
            }    
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
            // It's a row change event - nothing to do.
        }
    }
}
