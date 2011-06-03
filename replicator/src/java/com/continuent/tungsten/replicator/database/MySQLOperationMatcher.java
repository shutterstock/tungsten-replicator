/**
 * Tungsten: An Application Server for uni/cluster.
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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.database;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Parses SQL statements to extract the SQL operation and the object, identified
 * by type, name and schema, to which it pertains.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MySQLOperationMatcher implements SqlOperationMatcher
{
    private static Logger               logger          = Logger.getLogger(MySQLOperationMatcher.class);

    // Maximum length to search down large strings.
    private static int                  PREFIX_LENGTH   = 150;

    private MySQLOperationStringBuilder prefixBuilder;

    // CREATE {DATABASE | SCHEMA} [IF NOT EXISTS] db_name
    protected Pattern                   createDb        = Pattern
                                                                .compile(
                                                                        "^\\s*create\\s*(?:database|schema)\\s*(?:if\\s*not\\s*exists\\s*){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // DROP {DATABASE | SCHEMA} [IF EXISTS] db_name
    protected Pattern                   dropDb          = Pattern
                                                                .compile(
                                                                        "^\\s*drop\\s*(?:database|schema)\\s*(?:if\\s*exists\\s*)?[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
    protected Pattern                   createTable     = Pattern
                                                                .compile(
                                                                        "^\\s*create\\s*(?:temporary\\s*)?table\\s*(?:if\\s*not\\s*exists\\s*){0,1}(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // DROP [TEMPORARY] TABLE [IF EXISTS]
    protected Pattern                   dropTable       = Pattern
                                                                .compile(
                                                                        "^\\s*drop\\s*(?:temporary\\s*)?table\\s*(?:if\\s+exists\\s+)?(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE] [INTO] tbl_name
    protected Pattern                   insert          = Pattern
                                                                .compile(
                                                                        "^\\s*insert\\s*(?:low_priority|delayed|high_priority)?\\s*(?:ignore\\s*)?(?:into\\s*)?(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // REPLACE [LOW_PRIORITY | DELAYED] [INTO] tbl_name
    protected Pattern                   replace         = Pattern
                                                                .compile(
                                                                        "^\\s*replace\\s*(?:low_priority|delayed)?\\s*(?:into\\s*)?(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // UPDATE [LOW_PRIORITY] [IGNORE] table_reference
    protected Pattern                   update          = Pattern
                                                                .compile(
                                                                        "^\\s*update\\s*(?:low_priority\\s*)?(?:ignore\\s*)?(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
    protected Pattern                   delete          = Pattern
                                                                .compile(
                                                                        "^\\s*delete\\s*(?:low_priority\\s*)?(?:quick\\s*)?(?:ignore\\s*)?(?:from\\s*)?(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // TRUNCATE [TABLE] tbl_name
    protected Pattern                   truncate        = Pattern
                                                                .compile(
                                                                        "^\\s*truncate\\s*(?:table\\s*)?(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // LOAD DATA [LOW_PRIORITY | CONCURRENT] [LOCAL] INFILE 'file_name' [REPLACE
    // | IGNORE] INTO TABLE tbl_name
    protected Pattern                   loadData        = Pattern
                                                                .compile(
                                                                        "^\\s*load\\s*data.*(?:replace|ignore)?\\s*(?:local\\s*)?infile\\s.*(?:low_priority|concurrent)?\\s*into\\s*table\\s*(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // SET variable_assignment [, variable_assignment] ...
    protected Pattern                   set             = Pattern
                                                                .compile(
                                                                        "^\\s*set\\s*",
                                                                        Pattern.CASE_INSENSITIVE);
    // CREATE [DEFINER = { user | CURRENT_USER }] PROCEDURE name
    // ([param1[,...]])
    protected Pattern                   createProcedure = Pattern
                                                                .compile(
                                                                        "^\\s*create\\s*.*\\s*procedure\\s*{0,1}(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // DROP PROCEDURE [IF EXISTS]
    protected Pattern                   dropProcedure   = Pattern
                                                                .compile(
                                                                        "^\\s*drop\\s*procedure\\s*(?:if\\s*exists\\s*)?(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // CREATE [DEFINER = { user | CURRENT_USER }] FUNCTION name
    // ([param1[,...]])
    protected Pattern                   createFunction  = Pattern
                                                                .compile(
                                                                        "^\\s*create\\s*.*\\s*function\\s*{0,1}(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);
    // DROP PROCEDURE [IF EXISTS]
    protected Pattern                   dropFunction    = Pattern
                                                                .compile(
                                                                        "^\\s*drop\\s*function\\s*(?:if\\s+exists\\s*)?(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);

    // START TRANSACTION [WITH CONSISTENT SNAPSHOT] | BEGIN [WORK]
    protected Pattern                   begin           = Pattern
                                                                .compile(
                                                                        "^(begin|start)",
                                                                        Pattern.CASE_INSENSITIVE);

    // COMMIT [WORK]
    protected Pattern                   commit          = Pattern
                                                                .compile(
                                                                        "^(commit)*",
                                                                        Pattern.CASE_INSENSITIVE);

    // BEGIN ... END block
    protected Pattern                   beginEnd        = Pattern
                                                                .compile(
                                                                        "^begin\\s*.*\\s+end",
                                                                        Pattern.CASE_INSENSITIVE);

    // SELECT ... FROM table_references
    protected Pattern                   select          = Pattern
                                                                .compile(
                                                                        "^select",
                                                                        Pattern.CASE_INSENSITIVE);

    // ALTER [ONLINE | OFFLINE] [IGNORE] TABLE tbl_name ...
    protected Pattern                   alter           = Pattern
                                                                .compile(
                                                                        "^\\s*alter\\s*(?:online|offline)?\\s*(?:ignore\\s*)?table\\s+(?:[`\"]*([a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);

    /**
     * Create new instance.
     */
    public MySQLOperationMatcher()
    {
        prefixBuilder = new MySQLOperationStringBuilder(PREFIX_LENGTH);
    }

    /**
     * Examines a SQL DDL/DML statement and returns the name of the SQL object
     * it affects. To avoid unnecessary regex searches we test for the beginning
     * keyword of each expression.
     */
    public SqlOperation match(String inputStatement)
    {
        // Construct a prefix cleansed of leading whitespace and embedded
        // comments that we can use for efficient searching.
        String statement = prefixBuilder.build(inputStatement);
        String prefix = statement
                .substring(0, Math.min(statement.length(), 15)).toUpperCase();

        // Define a matcher instance and start looking...
        Matcher m;

        // Look for an insert statement.
        if (prefix.startsWith("INSERT"))
        {
            m = insert.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE,
                        SqlOperation.INSERT, m.group(1), m.group(2), false);
            }
        }

        // Look for a replace statement.
        else if (prefix.startsWith("REPLACE"))
        {
            m = replace.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE,
                        SqlOperation.REPLACE, m.group(1), m.group(2), false);
            }
        }

        // Look for an update statement.
        else if (prefix.startsWith("UPDATE"))
        {
            m = update.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE,
                        SqlOperation.UPDATE, m.group(1), m.group(2), false);
            }
        }

        // Look for a delete statement.
        else if (prefix.startsWith("DELETE"))
        {
            m = delete.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE,
                        SqlOperation.DELETE, m.group(1), m.group(2), false);
            }
        }

        // Look for a commit statement.
        else if (prefix.startsWith("COMMIT"))
        {
            m = commit.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TRANSACTION,
                        SqlOperation.COMMIT, null, null);
            }
        }

        // Look for a begin statement.
        else if (prefix.startsWith("BEGIN") || prefix.startsWith("START"))
        {
            // Begin ... end block.
            m = beginEnd.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.BLOCK,
                        SqlOperation.BEGIN_END, null, null, false);
            }
            // Begin transaction.
            m = begin.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TRANSACTION,
                        SqlOperation.BEGIN, null, null, false);
            }
        }

        // Look for a commit statement.
        else if (prefix.startsWith("SELECT"))
        {
            m = select.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE,
                        SqlOperation.SELECT, null, null, false);
            }
        }

        // Look for a set statement.
        else if (prefix.startsWith("SET"))
        {
            m = set.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.SESSION, SqlOperation.SET,
                        null, null);
            }
        }

        // Look for create commands.
        else if (prefix.startsWith("CREATE"))
        {
            // Create database.
            m = createDb.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.SCHEMA,
                        SqlOperation.CREATE, m.group(1), null);
            }
            // Create table.
            m = createTable.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE,
                        SqlOperation.CREATE, m.group(1), m.group(2));
            }
            // Create procedure.
            m = createProcedure.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.PROCEDURE,
                        SqlOperation.CREATE, m.group(1), m.group(2));
            }
            // Create function.
            m = createFunction.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.FUNCTION,
                        SqlOperation.CREATE, m.group(1), m.group(2));
            }
        }

        // Look for drop commands.
        else if (prefix.startsWith("DROP"))
        {
            // Drop database
            m = dropDb.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.SCHEMA, SqlOperation.DROP,
                        m.group(1), null);
            }
            // Drop table.
            m = dropTable.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE, SqlOperation.DROP,
                        m.group(1), m.group(2));
            }
            // Drop procedure.
            m = dropProcedure.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.PROCEDURE,
                        SqlOperation.DROP, m.group(1), m.group(2));
            }
            // Drop function.
            m = dropProcedure.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.PROCEDURE,
                        SqlOperation.DROP, m.group(1), m.group(2));
            }
        }

        // Look for a truncate statement.
        else if (prefix.startsWith("TRUNCATE"))
        {
            m = truncate.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE,
                        SqlOperation.TRUNCATE, m.group(1), m.group(2));
            }
        }

        // Look for a load data statement.
        else if (prefix.startsWith("LOAD"))
        {
            m = loadData.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE,
                        SqlOperation.LOAD_DATA, m.group(1), m.group(2));
            }
        }

        // Look for an ALTER statement
        else if (prefix.startsWith("ALTER"))
        {
            m = alter.matcher(statement);
            if (m.find())
            {
                return new SqlOperation(SqlOperation.TABLE, SqlOperation.ALTER,
                        m.group(1), m.group(2));
            }
        }

        // We didn't recognize anything.
        SqlOperation unrecognized = new SqlOperation(SqlOperation.UNRECOGNIZED,
                SqlOperation.UNRECOGNIZED, null, null, false);
        unrecognized.setBidiUnsafe(true);
        if (logger.isDebugEnabled())
        {
            logger.debug("Unrecognized SQL statement: " + inputStatement);
        }
        return unrecognized;
    }
}