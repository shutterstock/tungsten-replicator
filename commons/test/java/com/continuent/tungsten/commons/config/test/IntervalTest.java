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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.config.test;

import junit.framework.TestCase;

import com.continuent.tungsten.commons.config.Interval;

/**
 * Implements a unit test for time intervals, which are a Tungsten property
 * type.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class IntervalTest extends TestCase
{
    /**
     * Verify ability to create and return a time interval. 
     */
    public void testIntervalLong() throws Exception
    {
        Interval i = new Interval(0);
        assertEquals("Empty interval", 0, i.longValue());
        Interval i2 = new Interval(1000);
        assertEquals("Non-zero interval", 1000, i2.longValue());
    }
    
    /**
     * Verify ability to parse interval values from strings. 
     */
    public void testIntervalString() throws Exception
    {
        assertEquals(0, new Interval("0").longValue());
        assertEquals(100, new Interval("100").longValue());
        
        assertEquals(0, new Interval("0s").longValue());
        assertEquals(2000, new Interval("2s").longValue());
        assertEquals(2000, new Interval("2S").longValue());

        assertEquals(0, new Interval("0m").longValue());
        assertEquals(120000, new Interval(" 2m").longValue());
        assertEquals(120000, new Interval("2M").longValue());

        assertEquals(0, new Interval("0h").longValue());
        assertEquals(7200000, new Interval("2h ").longValue());
        assertEquals(7200000, new Interval("2H").longValue());

        assertEquals(0, new Interval("0d").longValue());
        assertEquals(7200000 * 24, new Interval(" 2d ").longValue());
        assertEquals(7200000 * 24, new Interval("2D").longValue());
    }
    
    /** 
     * Ensure that bad intervals generate exceptions. 
     */
    public void testBadValues() throws Exception
    {
        try
        {
            new Interval("100f").longValue();
            throw new Exception("Accepted bad unit");
        }
        catch (NumberFormatException e)
        {
        }
        try
        {
            new Interval("-100").longValue();
            throw new Exception("Accepted bad prefix");
        }
        catch (NumberFormatException e)
        {
        }
        try
        {
            new Interval("").longValue();
            throw new Exception("Accepted empty string");
        }
        catch (NumberFormatException e)
        {
        }
        try
        {
            new Interval("d").longValue();
            throw new Exception("Accepted unit only");
        }
        catch (NumberFormatException e)
        {
        }
    }
}