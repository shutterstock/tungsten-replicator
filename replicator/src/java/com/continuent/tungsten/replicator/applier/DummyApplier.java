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

import java.util.ArrayList;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

public class DummyApplier implements RawApplier
{
    int                      taskId             = 0;
    ArrayList<StatementData> trx                = null;
    ReplDBMSHeader           lastHeader         = null;
    boolean                  storeAppliedEvents = false;
    long                     eventCount         = 0;
    long                     txnCount           = 0;

    public void setStoreAppliedEvents(boolean store)
    {
        storeAppliedEvents = store;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    public void setTaskId(int id)
    {
        taskId = 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean, boolean)
     */
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit, boolean doRollback)
            throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        eventCount++;
        if (storeAppliedEvents)
        {
            for (DBMSData dataElem : data)
            {
                // TODO: Store other types as well.
                if (dataElem instanceof StatementData)
                    trx.add((StatementData) dataElem);
            }
        }
        if (doCommit)
        {
            lastHeader = header;
            txnCount++;
        }
    }

    public void commit()
    {
        // does nothing for now...
    }
    
    public void rollback() throws InterruptedException
    {
        // does nothing for now...
    }

    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        return lastHeader;
    }

    public void configure(PluginContext context)
    {

    }

    public void prepare(PluginContext context)
    {
        trx = new ArrayList<StatementData>();
    }

    public void release(PluginContext context)
    {
        trx = null;
    }

    public ArrayList<StatementData> getTrx()
    {
        return trx;
    }

    public long getEventCount()
    {
        return eventCount;
    }

    public long getTxnCount()
    {
        return txnCount;
    }
}
