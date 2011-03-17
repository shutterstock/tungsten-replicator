/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.consistency;

import java.io.Serializable;
import java.sql.ResultSet;
import com.continuent.tungsten.replicator.database.Database;

/**
 * This interface defines a ConsistencyCheck.
 * 
 * Each consistency check class represents a consistency check specification. It
 * consists of integer check ID, String schema and table names of a table to be
 * checked and a method that returns SELECT statment that actually does the
 * crc/count calculation for the given DBMS. Result set from SELECT should
 * contain integer field 'this_cnt' and String field 'this_crc'
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public interface ConsistencyCheck extends Serializable
{
    /**
     * Enumeration of supported methods
     */
    class Method
    {
        static final String MD5 = "md5";
    };

    /**
     * @return consistency check ID
     */
    int getCheckId();

    /**
     * @return schema of the checked table
     */
    String getSchemaName();

    /**
     * @return name of the checked table
     */
    String getTableName();

    /**
     * @return offset of the row the check starts with. 1st row has offset 0.
     *         Rows counted as sorted by primary key or by all columns if
     *         there's no primary key.
     */
    int getRowOffset();

    /**
     * @return how many rows to check
     */
    int getRowLimit();

    /**
     * @return String representation of a consistency check method
     */
    String getMethod();

    /**
     * @return ResultSet should contain at least two values: int 'this_cnt' and
     *         char[] 'this_crc'.
     */
    ResultSet performConsistencyCheck(Database conn)
            throws ConsistencyException;
}
