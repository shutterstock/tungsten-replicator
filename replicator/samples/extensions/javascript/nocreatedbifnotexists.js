/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * JavaScript example for JavaScriptFilter. Removes CREATE DATABASE IF NOT EXISTS
 * statements from the replication stream.
 * Targeted case: heterogeneous replication when one needs to remove Tungsten's
 * generated SQL like:
 * CREATE DATABASE IF NOT EXISTS tungsten_default
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.nocreatedbifnotexists=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.nocreatedbifnotexists.script=${replicator.home.dir}/samples/extensions/javascript/nocreatedbifnotexists.js
 * 
 * replicator.stage.thl-to-dbms.filters=...,nocreatedbifnotexists
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    beginning = "CREATE DATABASE IF NOT EXISTS";
	logger.info("nocreatedbifnotexists: statements beginning with \"" + beginning + "\" will be dropped");
}

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
    // Analise what this event is holding.
    data = event.getData();
	if(data != null)
	{
	    // One ReplDBMSEvent may contain many DBMSData events.
	    for (i = 0; i < data.size(); i++)
	    {
	        // Get com.continuent.tungsten.replicator.dbms.DBMSData
	        d = data.get(i);
	    
	        // Determine the underlying type of DBMSData event.
	        if(d != null && d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
			{
                sql = d.getQuery();
                if(sql.startsWith(beginning))
                {
                    data.remove(i);
					logger.debug("nocreatedbifnotexists: removed statement: " + d.getQuery());
					// As we removed the array element all other ones came closer to us:
					i--;
                }
	        }
	        else if(d != null && d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
	        {
                // Nothing to do.
	        }
	    }
		// Remove event completely, if everything's filtered out.
	    if (data.isEmpty())
	    {
	        return null;
	    }
	}
}
