/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * JavaScript example for JavaScriptFilter. Leaves only INSERT events from row
 * change events (does not touch statement events, thus DML statements either).
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.insertsonly=com.continuent.tungsten.replicator.filter.JavaScriptFilter                           
 * replicator.filter.insertsonly.script=../samples/extensions/javascript/insertsonly.js                               
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
            // It's a SQL statement event.    
            // Log the query into replicator's log.
            logger.info("StatementData: " + d.getQuery());
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
            // It's a row change event.
            logger.info("RowChangeData:");
        
            rowChanges = data.get(i).getRowChanges();
            
            // One RowChangeData may contain many OneRowChange events.
            for(j=0;j<rowChanges.size();j++)
            {
                // Get com.continuent.tungsten.replicator.dbms.OneRowChange
                oneRowChange = rowChanges.get(j);
        
                // Log action (INSERT, UPDATE, DELETE).
                logger.info(oneRowChange.getAction());
                
                // Skip any events that are not INSERTs.
                if(oneRowChange.getAction()!="INSERT")
                {
                    rowChanges.remove(j);
                    logger.info("Removed non-INSERT row change event @ " + j);
                    // As we removed the array element all other ones came closer to us:
                    j--;
                }
            }
        }
    }
}
