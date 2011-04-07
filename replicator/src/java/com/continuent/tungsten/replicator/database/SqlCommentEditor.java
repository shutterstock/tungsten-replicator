/**
 * Tungsten Clustering and Replication
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

/**
 * Denotes an interface to set and fetch comments in SQL statements.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface SqlCommentEditor
{
    /**
     * Inserts a comment safely into a SQL statement.
     * 
     * @param statement Statement that requires a comment to be inserted.
     * @param sqlOperation Metadata from parsing statement, if any
     * @param comment Comment string to be added.
     * @return Query with comment added
     */
    public String addComment(String statement, SqlOperation sqlOperation,
            String comment);

    /**
     * Formats an appendable comment if this works.
     * 
     * @param sqlOperation Metadata from parsing statement
     * @param comment Comment string to be added.
     * @return Appendable comment or null if such comments are not safe for
     *         current statement
     */
    public String formatAppendableComment(SqlOperation sqlOperation,
            String comment);

    /**
     * Set comment regex. This is used to fetch out specific parts of the
     * comment and is set once to avoid regex recompilation.
     */
    public void setCommentRegex(String regex);

    /**
     * Fetches the first comment string that matches the regex.
     * 
     * @param baseStatement Statement to search
     * @return Matching comment string or null if not found.
     */
    public String fetchComment(String baseStatement, SqlOperation sqlOperation);
}