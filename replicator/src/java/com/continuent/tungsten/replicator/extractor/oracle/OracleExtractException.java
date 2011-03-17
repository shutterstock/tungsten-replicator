/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Scott Martin
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.oracle;

import com.continuent.tungsten.replicator.extractor.ExtractorException;

/**
 * This class defines an OracleExtractException
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */

public class OracleExtractException extends ExtractorException
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new <code>OracleExtractException</code> object
     * 
     * @param message
     * @param cause
     */
    public OracleExtractException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Creates a new <code>OracleExtractException</code> object
     * 
     * @param message
     */
    public OracleExtractException(String message)
    {
        super(message);
    }

    /**
     * Creates a new <code>OracleExtractException</code> object
     * 
     * @param cause
     */
    public OracleExtractException(Throwable cause)
    {
        super(cause);
    }
}
