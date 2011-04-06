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

package com.continuent.tungsten.replicator.dbms;

import java.util.ArrayList;
import java.util.LinkedList;

import com.continuent.tungsten.replicator.event.ReplOption;

/**
 * This class defines a set of one or more row changes. 
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class RowChangeData extends DBMSData
{
	public enum ActionType {
		INSERT, DELETE, UPDATE
	}

	private static final long serialVersionUID = 1L;
	private ArrayList<OneRowChange> rowChanges;
	private LinkedList<ReplOption> options = new LinkedList<ReplOption>();
	
    /**
     * 
     * Creates a new <code>RowChangeData</code> object
     * 
     */
    public RowChangeData()
    {
    	super();
    	rowChanges = new ArrayList<OneRowChange>();
    }

	public ArrayList<OneRowChange> getRowChanges() {
		return rowChanges;
	}

	public void setRowChanges(ArrayList<OneRowChange> rowChanges) {
		this.rowChanges = rowChanges;
	}

	public void appendOneRowChange(OneRowChange rowChange) {
		this.rowChanges.add(rowChange);
	}

    public void addOptions(LinkedList<ReplOption> savedOptions)
    {
        this.options.addAll(savedOptions);
    }

    public LinkedList<ReplOption> getOptions()
    {
        return options;
    }
    

}
