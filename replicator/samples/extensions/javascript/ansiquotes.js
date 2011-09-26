/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * JavaScript example for JavaScriptFilter.
 *
 * Enables ANSI_QUOTES mode for incoming events.
 * Use case example: heterogeneous replication.
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.ansiquotes=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.ansiquotes.script=${replicator.home.dir}/samples/extensions/javascript/ansiquotes.js
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
    logger.info("ansiquotes: will be adding ANSI_QUOTES mode for each event");
}

function filter(event)
{
    // Analyse what this event is holding.
    data = event.getData();

    // Turn on ANSI_QUOTES mode.
    query = "SET sql_mode='ANSI_QUOTES'";
    newStatement = new com.continuent.tungsten.replicator.dbms.StatementData(
         query,
         null,
         null
         );
    // Add new statement before all other events.
    data.add(0, newStatement);
    if(logger.isDebugEnabled())
        logger.debug("Added: " + query);
    // Move on (all array shifted right after element addition).
    //i++;
        
    // One ReplDBMSEvent may contain many DBMSData events.
    for(i = 0; i < data.size(); i++)
    {
        // Get com.continuent.tungsten.replicator.dbms.DBMSData
        d = data.get(i);
   
        // Determine the underlying type of DBMSData event.
        if(d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
        {
            // It's a SQL statement event.
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
            // It's a row change event.
        }
    }
    
    // Turn off ANSI_QUOTES mode so internal trep_commit_seqno update would work.
    query = "SET sql_mode=''";
    newStatement = new com.continuent.tungsten.replicator.dbms.StatementData(
         query,
         null,
         null
         );
    // Add new statement before all other events.
    data.add(data.size(), newStatement);
    if(logger.isDebugEnabled())
        logger.debug("Added: " + query);

}
