/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * JavaScript example for JavaScriptFilter. For every
 * CREATE|DROP|ALTER|RENAME TABLE event adds a "SET foreign_key_checks=0"
 * statement. This is a workaround for TREP-70 while it is being fixed.
 * It is intended for row replication as statement replication might
 * contain "CREATE TABLE" sentence in DML statements (eg. in some programming
 * forum database).
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.postfilter=foreignkeychecks
 *
 * replicator.filter.foreignkeychecks=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.foreignkeychecks.script=../samples/extensions/javascript/foreignkeychecks.js
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
            // Is it a CREATE TABLE statement?
            upCaseQuery = d.getQuery().trim().toUpperCase();
            if(upCaseQuery.startsWith("CREATE TABLE") ||
                upCaseQuery.startsWith("DROP TABLE") ||
                upCaseQuery.startsWith("ALTER TABLE") ||
                upCaseQuery.startsWith("RENAME TABLE")
            )
            {
                // Turn off the foreign key checking.
                query = "SET foreign_key_checks=0";
                newStatement = new com.continuent.tungsten.replicator.dbms.StatementData(
                     d.getDefaultSchema(),
                     null,
                     query
                     );
                // Add new statement before all other events.
                data.add(0, newStatement);
                logger.info("Added: SET foreign_key_checks=0");
                // Do not read the CREATE TABLE another time.
                i++;
            }
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
            // It's a row change event.
        }
    }
}
