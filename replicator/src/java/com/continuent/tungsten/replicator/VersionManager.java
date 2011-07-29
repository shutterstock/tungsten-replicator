/**
 * Tungsten Scale-Out Stack
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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s): Robert Hodges, Stephane Giron.
 */

package com.continuent.tungsten.replicator;

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.management.OpenReplicatorManager;
import com.continuent.tungsten.replicator.thl.THL;

/**
 * This class defines methods to store ans retrieve module versions
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class VersionManager
{
    private String        url            = null;
    private String        user           = null;
    private String        password       = null;
    private String        metadataSchema = null;
    private Database      conn           = null;
    private Statement     statement      = null;

    private static Logger logger         = Logger.getLogger(VersionManager.class);

    private void connect(String url, String user, String password)
            throws SQLException
    {
        conn = DatabaseFactory.createDatabase(url, user, password);
        conn.connect();
        statement = conn.createStatement();
    }

    private void disconnect() throws SQLException
    {
        if (conn != null)
        {
            if (statement != null)
            {
                statement.close();
            }
            conn.close();
        }
    }

    public VersionManager(TungstenProperties properties) throws SQLException,
            ReplicatorException
    {
        this.metadataSchema = properties
                .getString(ReplicatorConf.METADATA_SCHEMA);
        // this.dbms = JdbcURL.string2Dbms(properties
        // .getString(ReplicatorConf.THL_DB_DRIVER));
        // String host = properties.getString(ReplicatorConf.THL_DB_HOST);
        // int port = properties.getInt(ReplicatorConf.THL_DB_PORT);
        // this.url = JdbcURL.generate(dbms, host, port, null);
        this.url = properties.getString(ReplicatorConf.THL_DB_URL);
        this.user = properties.getString(ReplicatorConf.THL_DB_USER);
        this.password = properties.getString(ReplicatorConf.THL_DB_PASSWORD);

        // Create version table if it does not exist.
        Table version = Version.getVersionTableDefinition(this.metadataSchema);
        connect(url, user, password);
        conn.createTable(version, false, properties.getString(
                ReplicatorConf.METADATA_TABLE_TYPE,
                ReplicatorConf.METADATA_TABLE_TYPE_DEFAULT, false));
        disconnect();
    }

    public Version getVersion(String module) throws SQLException
    {
        Version ret = null;

        connect(url, user, password);

        try
        {
            statement.setFetchSize(1);
            ret = Version.getVersionFromDB(statement, metadataSchema, module);
        }
        finally
        {
            disconnect();
        }

        return ret;
    }

    public void setVersion(String module, Version version) throws SQLException
    {
        connect(url, user, password);

        try
        {
            Version.saveVersionToDB(statement, metadataSchema, module, version);
        }
        finally
        {
            disconnect();
        }
    }

    private void checkModule(String module, Version new_ver)
            throws SQLException
    {
        Version old_ver = getVersion(module);

        if (old_ver != null)
        {
            logger.info("Found '" + module + "' verstion: "
                    + old_ver.toString());
            // if (old_ver.compare(new_ver) == 0) return;
        }

        logger.info("Setting '" + module + "' verstion to "
                + new_ver.toString());
        setVersion(module, new_ver);
    }

    /**
     * TODO: checks tungsten module versions
     */
    public void check() throws SQLException
    {
        checkModule("Replicator", new Version(OpenReplicatorManager.MAJOR,
                OpenReplicatorManager.MINOR, OpenReplicatorManager.SUFFIX));

        checkModule("THL", new Version(THL.MAJOR, THL.MINOR, THL.SUFFIX));
    }
}
