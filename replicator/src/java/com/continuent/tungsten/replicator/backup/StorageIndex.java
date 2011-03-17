/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
package com.continuent.tungsten.replicator.backup;

import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * Contains index to storage files.  The index is used to generate new
 * file numbers. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class StorageIndex
{
    // Property serialization information. 
    private static final String VERSION_NO = "1.0";
    private static final String VERSION = "version";
    private static final String INDEX = "index";
    
    // Specification values. 
    private String version;
    private long index;
    
    /**
     * Creates an index specification from existing properties. 
     */
    public StorageIndex(TungstenProperties props)
    {
        this.version = props.getString(VERSION);
        this.index = props.getLong(INDEX);
    }
 
    /**
     * Creates a new specification whose values must be filled in. 
     */
    public StorageIndex()
    {
        this.version = VERSION_NO;
    }

    public void incrementIndex()
    {
        index++;
    }

    public long getIndex()
    {
        return index;
    }

    public void setIndex(long index)
    {
        this.index = index;
    }

    public String getVersion()
    {
        return version;
    }
    
    public TungstenProperties toProperties()
    {
        TungstenProperties props = new TungstenProperties();
        props.setString(VERSION, version);
        props.setLong(INDEX, index);
        return props;
    }
}