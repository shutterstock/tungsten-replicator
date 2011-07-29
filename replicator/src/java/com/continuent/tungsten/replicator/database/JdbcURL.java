/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Contributor(s): Marcus Eriksson, Linas Virbalas
 * DRIZZLE CODE DONATED UNDER TUNGSTEN CODE CONTRIBUTION AGREEMENT
 */

package com.continuent.tungsten.replicator.database;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a JdbcURL DBMS URL generator
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class JdbcURL
{
    
    public static String generate (DBMS dbms, String host, int port, String service) {
        
        StringBuffer sb = new StringBuffer();
        
        switch (dbms) {
            case MYSQL:
                sb.append("jdbc:mysql://"); 
                sb.append(host);
                if (port > 0)
                {
                    sb.append(":");
                    sb.append(port);
                }
                sb.append("/");
                break;
            case DRIZZLE:
                sb.append("jdbc:drizzle://");
                sb.append(host);
                if (port > 0)
                {
                    sb.append(":");
                    sb.append(port);
                }
                sb.append("/");
                break;
            case ORACLE:
                sb.append("jdbc:oracle:thin:@");
                sb.append(host);
                sb.append(":");
                sb.append(port);
                sb.append(":");
                sb.append(service);
                break;
            case DERBY:
                // FIXME: this may be not right for the intended derby use.
                sb.append("jdbc:derby:");
                sb.append(host);
                break;
            case POSTGRESQL:
                sb.append("jdbc:postgresql://"); 
                sb.append(host);
                if (port > 0)
                {
                    sb.append(":");
                    sb.append(port);
                }
                sb.append("/");
                break;
            case GREENPLUM:
                sb.append("jdbc:postgresql://"); 
                sb.append(host);
                if (port > 0)
                {
                    sb.append(":");
                    sb.append(port);
                }
                sb.append("/");
                break;
        }
        return sb.toString();
    }
    
    public static DBMS string2DBMS (String driver_spec) throws ReplicatorException
    {
        if (driver_spec.toLowerCase().matches("mysql")) return DBMS.MYSQL;
        if (driver_spec.toLowerCase().matches("drizzle")) return DBMS.DRIZZLE;        
        if (driver_spec.toLowerCase().matches("oracle")) return DBMS.ORACLE;
        if (driver_spec.toLowerCase().matches("derby")) return DBMS.DERBY;
        throw new DatabaseException ("driver '" + driver_spec + "' not supported.");
    }
}
