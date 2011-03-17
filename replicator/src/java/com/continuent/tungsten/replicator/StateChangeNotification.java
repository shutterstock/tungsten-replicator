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

package com.continuent.tungsten.replicator;

import java.io.Serializable;

/**
 * 
 * This class defines a StateChangeNotification
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class StateChangeNotification implements Serializable
{
    
    static final long serialVersionUID = 1L;
    String prevState = null;
    String newState = null;
    String cause = null;
    /**
     * 
     * Creates a new <code>StateChangeNotification</code> object
     * 
     * @param prevState
     * @param newState
     * @param cause
     */
    public StateChangeNotification(String prevState, String newState, String cause)
    {
       this.prevState = prevState;
       this.newState = newState;
       this.cause = cause;
    }
    
    /**
     * 
     * TODO: getPrevState definition.
     * 
     * @return prevState
     */
    public String getPrevState()
    {
        return prevState;
    }
    
    /**
     * 
     * TODO: getNewState definition.
     * 
     * @return newState
     */
    public String getNewState()
    {
        return newState;
    }
    
    /**
     * 
     * TODO: getCause definition.
     * 
     * @return cause
     */
    public String getCause()
    {
        return cause;
    }
    
}
