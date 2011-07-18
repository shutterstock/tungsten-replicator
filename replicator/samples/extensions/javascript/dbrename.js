/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * JavaScript example for JavaScriptFilter.
 *
 * Allows one to rename the schema of an operation to a new schema
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.dbrename=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.dbrename.script=../samples/extensions/javascript/dbrename.js
 * replicator.filter.dbrename.dbsource=SOURCE
 * replicator.filter.dbrename.dbtarger=TEST
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
    sourceName = filterProperties.getString("dbsource");
    targetName = filterProperties.getString("dbtarget");
    logger.info("dbrename: rename statements from schema \"" + sourceName + "\" to schema \"" +
                 targetName + "\"");
}

function filter(event)
{
    sourceName = filterProperties.getString("dbsource");
    targetName = filterProperties.getString("dbtarget");
    
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
            if(d.getDefaultSchema() != null && d.getDefaultSchema().compareTo(sourceName)==0)
            {
                logger.debug("dbrename: renaming schema from \"" + sourceName + "\" to \"" + targetName);
                d.setDefaultSchema(targetName);
            }
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
            // It's a row change event.
            rowChanges = data.get(i).getRowChanges();
            
            // One RowChangeData may contain many OneRowChange events.
            for(j=0;j<rowChanges.size();j++)
            {
                // Get com.continuent.tungsten.replicator.dbms.OneRowChange
                oneRowChange = rowChanges.get(j);
                
                if(oneRowChange.getSchemaName().compareTo(sourceName)==0)
                {
                    oneRowChange.setSchemaName(targetName);

                    logger.debug("dbrename: renaming schema from \"" + sourceName + "\" to \"" + targetName);
                }
            }
        }
    }
}
