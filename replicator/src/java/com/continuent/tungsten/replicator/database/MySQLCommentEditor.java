/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2011 Continuent Inc.
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

/**
 * Parses SQL statements to extract the SQL operation and the object, identified
 * by type, name and schema, to which it pertains.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MySQLCommentEditor implements SqlCommentEditor
{
    protected Pattern             standardPattern;
    protected Pattern             sprocPattern;
    protected SqlOperationMatcher sqlMatcher = new MySQLOperationMatcher();

    @Override
    public String addComment(String statement, SqlOperation sqlOp,
            String comment)
    {
        // Look for a stored procedure or function creation.
        if (sqlOp.getOperation() == SqlOperation.CREATE)
        {
            int objectType = sqlOp.getObjectType();
            if (objectType == SqlOperation.PROCEDURE
                    || objectType == SqlOperation.FUNCTION)
            {
                // Processing for CREATE PROCEDURE/FUNCTION -- add a COMMENT.
                // Following regex splits on line boundaries.
                String[] lines = statement.split("(?m)$");
                StringBuffer sb = new StringBuffer();
                boolean hasComment = false;
                for (String line : lines)
                {
                    String uline = line.toUpperCase();
                    if (uline.indexOf("COMMENT") > -1)
                    {
                        sb.append("    COMMENT '" + comment + "'");
                        hasComment = true;
                    }
                    else if (uline.indexOf("BEGIN") > -1)
                    {
                        if (!hasComment)
                        {
                            sb.append("    COMMENT '" + comment + "'");
                            hasComment = true;
                        }
                        sb.append(line);
                    }
                    else
                        sb.append(line);
                }
                return sb.toString();
            }
        }

        // For any others just append the comment.
        return statement + " /* " + comment + " */";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.SqlCommentEditor#formatAppendableComment(java.lang.String)
     */
    @Override
    public String formatAppendableComment(SqlOperation sqlOp, String comment)
    {
        // Look for a stored procedure or function and return null. They are not
        // safe for appending.
        if (sqlOp.getOperation() == SqlOperation.CREATE)
        {
            int objectType = sqlOp.getObjectType();
            if (objectType == SqlOperation.PROCEDURE
                    || objectType == SqlOperation.FUNCTION)
            {
                return null;
            }
        }

        // For any others return a properly formatted comment.
        return " /* " + comment + " */";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.SqlCommentEditor#fetchComment(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public String fetchComment(String statement, SqlOperation sqlOp)
    {
        // Select correct comment pattern.
        Pattern commentPattern = standardPattern;
        if (sqlOp.getOperation() == SqlOperation.CREATE)
        {
            int objectType = sqlOp.getObjectType();
            if (objectType == SqlOperation.PROCEDURE)
                commentPattern = sprocPattern;
            else if (objectType == SqlOperation.FUNCTION)
                commentPattern = sprocPattern;
        }

        // Look for pattern match and return value if found.
        Matcher m = commentPattern.matcher(statement);
        if (m.find())
            return m.group(1);
        else
            return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.SqlCommentEditor#setCommentRegex(java.lang.String)
     */
    @Override
    public void setCommentRegex(String regex)
    {
        String standardRegex = "\\/\\* (" + regex + ") \\*\\/";
        String sprocRegex = "COMMENT\\s*'(" + regex + ").*'";
        standardPattern = Pattern.compile(standardRegex);
        sprocPattern = Pattern.compile(sprocRegex);
    }
}