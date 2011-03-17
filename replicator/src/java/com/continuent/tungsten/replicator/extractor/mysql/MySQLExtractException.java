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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import com.continuent.tungsten.replicator.extractor.ExtractorException;

/**
 * This class defines a MySQLExtractException
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class MySQLExtractException extends ExtractorException
{
    private static final long serialVersionUID = 1L;

    public MySQLExtractException(String message)
    {
        super(message);
    }

    public MySQLExtractException(Throwable cause)
    {
        super(cause);
    }

    public MySQLExtractException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public MySQLExtractException(String message, Throwable cause, String eventId)
    {
        super(message, cause);
    }
}
