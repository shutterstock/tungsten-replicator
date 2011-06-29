/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.applier;

import java.sql.Timestamp;
import java.util.ArrayList;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.plugin.PluginLoader;

/**
 * Implements a simple test to ensure dummy applier work.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class TestApplierPlugin extends TestCase
{

    static Logger logger = null;

    public void setUp() throws Exception
    {
        if (logger == null)
            logger = Logger.getLogger(TestApplierPlugin.class);
    }

    public void testApplierBasic() throws Exception
    {
        RawApplier applier = (RawApplier) PluginLoader.load(DummyApplier.class
                .getName());
        applier.prepare(null);
        ((DummyApplier) applier).setStoreAppliedEvents(true);

        for (Integer i = 0; i < 10; ++i)
        {
            ArrayList<DBMSData> sql = new ArrayList<DBMSData>();
            sql.add(new StatementData("SELECT " + i));
            Timestamp now = new Timestamp(System.currentTimeMillis());
            applier.apply(new DBMSEvent(i.toString(), sql, now),
                    new ReplDBMSHeaderData(i, (short) 0, true, "test", 0,
                            "test", "myshard", now), true, false);
        }

        ArrayList<StatementData> sql = ((DummyApplier) applier).getTrx();
        for (int i = 0; i < 10; ++i)
        {
            Assert.assertEquals("SELECT " + i, sql.get(i).getQuery());
        }

        applier.release(null);

    }
}
