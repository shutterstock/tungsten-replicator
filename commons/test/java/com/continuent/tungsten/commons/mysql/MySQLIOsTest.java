/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Initial developer(s): Gilles Rayrat
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.mysql;

import java.sql.Connection;
import java.sql.DriverManager;

import junit.framework.TestCase;

/**
 * Tests MySQLIOs by creating a connection and .
 * 
 * @author <a href="gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class MySQLIOsTest extends TestCase
{
    public void testDummy()
    {
        // I'm just here for junit to be happy and find a test.
        // My friends below require some work since they need a host to connect
        // to and should be extended to read and write data to the database
    }

    public void DISABLED_testGetIOsForDrizzle() throws Exception
    {
        Class.forName("org.drizzle.jdbc.DrizzleDriver").newInstance();

        Connection conn = DriverManager
                .getConnection("jdbc:mysql:thin://tungsten:secret@localhost/test");

        MySQLIOs.getMySQLIOs(conn);
        conn.close();
    }
    public void DISABLED_testGetIOsForMySQL() throws Exception
    {
        Class.forName("com.mysql.jdbc.Driver").newInstance();

        Connection conn = DriverManager
                .getConnection("jdbc:mysql://localhost/test", "tungsten", "secret");

        MySQLIOs.getMySQLIOs(conn);
        conn.close();
    }
}
