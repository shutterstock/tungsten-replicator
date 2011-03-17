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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.event;

import java.io.Serializable;

/**
 * This class stores generic name/value pairs in an easily serializable
 * format.  It provides an standard way to represent metadata and 
 * session variables. 
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ReplOption implements Serializable
{
    private static final long serialVersionUID = 1L;
    
    private String name ="";
    private String value = "";
    
    /**
     * Creates a new <code>StatementDataOption</code> object
     * 
     * @param option
     * @param value 
     */
    public ReplOption(String option, String value)
    {
        this.name = option;
        this.value  = value;
    }

    /**
     * Returns the name value.
     * 
     * @return Returns the name.
     */
    public String getOptionName()
    {
        return name;
    }

    /**
     * Returns the value value.
     * 
     * @return Returns the value.
     */
    public String getOptionValue()
    {
        return value;
    }

    /**
     * {@inheritDoc}
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return name + " = " + value;
    }
}
