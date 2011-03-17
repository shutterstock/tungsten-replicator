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
 * Initial developer(s): Edward Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.utils;

/**
 * This class defines a DoubleStatistic
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class DoubleStatistic implements Statistic<Double>
{
    String label;
    Double value = new Double(0);
    long   count;
    
    public DoubleStatistic(String label)
    {
        this.label = label;
    }

    public Double decrement()
    {
        return value -= 1;
    }

    public Double getAverage()
    {
        if (count > 0)
        {
            return value / count;
        }

        return value;
    }

    public String getLabel()
    {
        return label;
    }

    public Double getValue()
    {
        return value;
    }

    public Double increment()
    {
        return value += 1;
    }

    public void setValue(Number value)
    {
        this.value = value.doubleValue();
    }

    public Double add(Number value)
    {
        count++;
        return this.value += value.longValue();
    }

    public Double subtract(Number value)
    {
        return this.value -= value.doubleValue();
    }
    
    public String toString()
    {
        return value.toString();
    }
    
    public void clear()
    {
        value = new Double(0);
        count = 0;
    }
}
