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
 * Contributor(s):
 */

package com.continuent.tungsten.commons.config.test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * This class is used to test setting property values.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SampleObject
{
    private String       string;
    private int          myInt;
    private long         myLong;
    private float        myFloat;
    private double       myDouble;
    private boolean      myBoolean;
    private char         myChar;
    private Date         myDate;
    private BigDecimal   myBigDecimal;
    private SampleEnum   myEnum;
    private List<String> myStringList;

    public SampleObject()
    {
    }

    public String getString()
    {
        return string;
    }

    public void setString(String string)
    {
        this.string = string;
    }

    public int getMyInt()
    {
        return myInt;
    }

    public void setMyInt(int myInt)
    {
        this.myInt = myInt;
    }

    public long getMyLong()
    {
        return myLong;
    }

    public void setMyLong(long myLong)
    {
        this.myLong = myLong;
    }

    public float getMyFloat()
    {
        return myFloat;
    }

    public void setMyFloat(float myFloat)
    {
        this.myFloat = myFloat;
    }

    public double getMyDouble()
    {
        return myDouble;
    }

    public void setMyDouble(double myDouble)
    {
        this.myDouble = myDouble;
    }

    public boolean isMyBoolean()
    {
        return myBoolean;
    }

    public void setMyBoolean(boolean myBoolean)
    {
        this.myBoolean = myBoolean;
    }

    public char getMyChar()
    {
        return myChar;
    }

    public void setMyChar(char myChar)
    {
        this.myChar = myChar;
    }

    public Date getMyDate()
    {
        return myDate;
    }

    public void setMyDate(Date myDate)
    {
        this.myDate = myDate;
    }

    public BigDecimal getMyBigDecimal()
    {
        return this.myBigDecimal;
    }

    public void setMyBigDecimal(BigDecimal myBigDecimal)
    {
        this.myBigDecimal = myBigDecimal;
    }

    public SampleEnum getMyEnum()
    {
        return this.myEnum;
    }

    public void setMyEnum(SampleEnum myEnum)
    {
        this.myEnum = myEnum;
    }

    public List<String> getMyStringList()
    {
        return this.myStringList;
    }

    public void setMyStringList(List<String> myStringList)
    {
        this.myStringList = myStringList;
    }

    public boolean equals(Object o)
    {
        if (o == null)
            return false;
        if (!(o instanceof SampleObject))
            return false;
        SampleObject to = (SampleObject) o;
        if (string == null)
        {
            if (to.getString() != null)
                return false;
        }
        else
        {
            if (!string.equals(to.getString()))
                return false;
        }
        if (myInt != to.getMyInt())
            return false;
        if (myLong != to.getMyLong())
            return false;
        if (myFloat != to.getMyFloat())
            return false;
        if (myDouble != to.getMyDouble())
            return false;
        if (myBoolean != to.isMyBoolean())
            return false;
        if (myChar != to.getMyChar())
            return false;
        if (myDate != to.getMyDate())
            return false;
        if (myBigDecimal != to.getMyBigDecimal())
            return false;
        if (myEnum != to.getMyEnum())
            return false;
        if (myStringList == null)
        {
            if (to.getMyStringList() != null)
                return false;
        }
        else
        {
            if (!myStringList.equals(to.getMyStringList()))
                return false;
        }
        return true;
    }

    public enum SampleEnum
    {
        ONE, TWO, THREE
    }
}