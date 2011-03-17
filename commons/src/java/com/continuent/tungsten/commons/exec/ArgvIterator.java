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
 * Initial developer(s): Robert Hodges
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.commons.exec;

import java.util.Iterator;

/**
 * Simple iterator class for argv arrays.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ArgvIterator implements Iterator<String>
{
    String argv[];
    int    index;

    public ArgvIterator(String[] argv)
    {
        this.argv = argv;
        index = 0;
    }

    public ArgvIterator(String[] argv, int index)
    {
        this.argv = argv;
        this.index = index;
    }

    public boolean hasNext()
    {
        return (index < argv.length);
    }

    public String next()
    {
        return argv[index++];
    }

    public void remove()
    {
        // Do nothing.
    }
    
    public String peek()
    {
        if (hasNext())
            return argv[index];
        else
            return null;
    }
}