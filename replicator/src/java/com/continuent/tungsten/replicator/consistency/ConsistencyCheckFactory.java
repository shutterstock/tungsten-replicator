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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.consistency;

import com.continuent.tungsten.replicator.database.Table;

/**
 * ConsistencyCheckFactory creates ConsistencyCheck objects
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class ConsistencyCheckFactory
{
    public static ConsistencyCheck createConsistencyCheck(int id, Table table,
            int rowOffset, int rowLimit, String method,
            boolean checkColumnNames, boolean checkColumnTypes)
            throws ConsistencyException
    {
        if (method.compareToIgnoreCase(ConsistencyCheck.Method.MD5) == 0)
        {
            return new ConsistencyCheckMD5(id, table, rowOffset, rowLimit,
                    checkColumnNames, checkColumnTypes);
        }
        else
        {
            throw new ConsistencyException(
                    "Unsupported consistency check method: '" + method + "'");
        }
    }
}
