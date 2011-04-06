/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Scott Martin
 * Contributor(s): Marcus Eriksson, Linas Virbalas, Stephane Giron
 * DRIZZLE CODE DONATED UNDER TUNGSTEN CODE CONTRIBUTION AGREEMENT
 */

package com.continuent.tungsten.replicator.database;

import java.sql.SQLException;

/**
 * This class defines a DatabaseFactory
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class DatabaseFactory
{
    static public Database createDatabase(String url, String user,
            String password) throws SQLException
    {
        return createDatabase(url, user, password, null);
    }
    
    static public Database createDatabase(String url, String user,
            String password, String vendor) throws SQLException
    {
        Database database;
        if (url.startsWith("jdbc:drizzle"))
            database = new DrizzleDatabase();
        else if (url.startsWith("jdbc:mysql:thin"))
            database = new MySQLDrizzleDatabase();
        else if (url.startsWith("jdbc:mysql"))
            database = new MySQLDatabase();
        else if (url.startsWith("jdbc:oracle"))
            database = new OracleDatabase();
        else if (url.startsWith("jdbc:derby"))
            database = new DerbyDatabase();
        else if (url.startsWith("jdbc:postgresql")
                && (vendor == null || vendor.equals("postgresql")))
            database = new PostgreSQLDatabase();
        else if (url.startsWith("jdbc:postgresql")
                && (vendor != null && vendor.equals("greenplum")))
            database = new GreenplumDatabase();
        else
            throw new RuntimeException("Unsupported URL type: " + url);

        database.setUrl(url);
        database.setUser(user);
        database.setPassword(password);

        return database;
    }

}
