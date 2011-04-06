/**
 * Tungsten Scale-Out Stack
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.event;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.continuent.tungsten.replicator.dbms.DBMSData;

/**
 * Contains SQL row updates and/or statements that must be replicated.
 * Extractors generate updates using this class and appliers receive updates in
 * it. Each instance is implicitly a single transaction.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class DBMSEvent implements Serializable
{
    private static final long      serialVersionUID = 1300L;
    private String                 id;
    private LinkedList<ReplOption> metadata;
    private ArrayList<DBMSData>    data;
    private boolean                lastFrag;
    private Timestamp              sourceTstamp;
    private LinkedList<ReplOption> options;

    /**
     * Creates a new instance of raw replicated data.
     * 
     * @param id Native transaction ID
     * @param metadata List of name/value pairs containing metadata about this
     *            event
     * @param data List of SQL statements or row updates
     * @param lastFrag True if this is the last fragment of a transaction
     * @param sourceTstamp Time of the transaction
     */
    public DBMSEvent(String id, LinkedList<ReplOption> metadata,
            ArrayList<DBMSData> data, boolean lastFrag, Timestamp sourceTstamp)
    {
        // Eliminate all possibilities of null pointers.
        if (id == null)
            this.id = "NIL";
        else
            this.id = id;
        if (metadata == null)
            this.metadata = new LinkedList<ReplOption>();
        else
            this.metadata = metadata;
        if (data == null)
            this.data = new ArrayList<DBMSData>();
        else
            this.data = data;
        this.lastFrag = lastFrag;
        if (sourceTstamp == null)
            this.sourceTstamp = new Timestamp(System.currentTimeMillis());
        else
            this.sourceTstamp = sourceTstamp;
        options = new LinkedList<ReplOption>();
    }

    public DBMSEvent(String id, ArrayList<DBMSData> data, Timestamp sourceTstamp)
    {
        this(id, new LinkedList<ReplOption>(), data, true, sourceTstamp);
    }

    public DBMSEvent(String id, ArrayList<DBMSData> data, boolean lastFrag,
            Timestamp sourceTstamp)
    {
        this(id, new LinkedList<ReplOption>(), data, lastFrag, sourceTstamp);
    }

    public DBMSEvent(String id, LinkedList<ReplOption> metadata,
            ArrayList<DBMSData> data, Timestamp sourceTstamp)
    {
        this(id, metadata, data, true, sourceTstamp);
    }

    /**
     * Constructor for dummy DBMSEvent with an event ID only; all other values
     * are defaults
     */
    public DBMSEvent(String id)
    {
        this(id, null, null, true, null);
    }

    /**
     * Constructor for dummy DBMSEvents. All values are defaults.
     */
    public DBMSEvent()
    {
        this(null, null, null, true, null);
    }

    /**
     * Returns the native event ID.
     * 
     * @return id
     */
    public String getEventId()
    {
        return id;
    }

    /**
     * Returns the metadata options.
     * 
     * @return metadata
     */
    public LinkedList<ReplOption> getMetadata()
    {
        return metadata;
    }

    /**
     * Adds a metadata option, which is assumed not to exist previously.
     */
    public void addMetadataOption(String name, String value)
    {
        metadata.add(new ReplOption(name, value));
    }

    /** 
     * Sets an existing metadata option or if absent adds it. 
     */
    public void setMetaDataOption(String name, String value)
    {
        for (int i = 0; i < metadata.size(); i++)
        {
            ReplOption option = metadata.get(i);
            if (name.equals(option.getOptionName()))
            {
                metadata.set(i, new ReplOption(name, value));
                return;
            }
        }
        addMetadataOption(name, value);
    }

    /**
     * Gets a metadata option.
     */
    public ReplOption getMetadataOption(String name)
    {
        for (ReplOption option : metadata)
        {
            if (name.equals(option.getOptionName()))
                return option;
        }
        return null;
    }

    /**
     * Gets a metadata value..
     */
    public String getMetadataOptionValue(String name)
    {
        for (ReplOption option : metadata)
        {
            if (name.equals(option.getOptionName()))
                return option.getOptionValue();
        }
        return null;
    }

    /**
     * Returns all database updates.
     * 
     * @return data
     */
    public ArrayList<DBMSData> getData()
    {
        return data;
    }

    /**
     * Returns true if this is the last fragment of a transaction.
     */
    public boolean isLastFrag()
    {
        return lastFrag;
    }

    /**
     * Returns the source timestamp, i.e., when the transaction occurred.
     * 
     * @return Returns the sourceTstamp.
     */
    public Timestamp getSourceTstamp()
    {
        return sourceTstamp;
    }

    public void setOptions(LinkedList<ReplOption> savedOptions)
    {
        this.options.addAll(savedOptions);
    }

    public List<ReplOption> getOptions()
    {
        return options;
    }

    public void addOption(String name, String value)
    {
        options.add(new ReplOption(name, value));
    }

}
