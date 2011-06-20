/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * JavaScript example for JavaScriptFilter.

 * Reformats MySQL's DDL statements to support PostgreSQL slave. Currently reformats only:
 * 1. " integer autoincrement " -> " serial "
 * 2. " tinyint" -> " smallint"
 *
 * NOTE: Case sensitive!
 * NOTE: Extend as needed per case by case basis.
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    transformers = new Array();
	transformers[0] = new Array(2);
	transformers[0][0] = " integer auto_increment ";
	transformers[0][1] = " serial ";
	transformers[1] = new Array(2);
	transformers[1][0] = " tinyint";
	transformers[1][1] = " smallint";
	
	for (t = 0; t < transformers.length; t++)
		logger.debug("pgddl: " + transformers[t][0] + " -> " + transformers[t][1]);
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
			for (t = 0; t < transformers.length; t++)
			{
				sql = d.getQuery();
				if(sql.indexOf(transformers[t][0]) != -1)
				{
					newSql = sql.replace(transformers[t][0], transformers[t][1]);
					logger.debug("pgddl: " + sql + " -> " + newSql);
					d.setQuery(newSql);
				}
			}
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
			// DDL does not come as a row change.
        }
    }
}
