/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.database;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Tests the MySQLOperationStringBuilder used to strip comments and leading
 * white space from MySQL statements.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestMySQLStringBuilder
{
    /**
     * Ensure leading whitespace is removed.
     */
    @Test
    public void testLeadingWhitespace() throws Exception
    {
        MySQLOperationStringBuilder sb = new MySQLOperationStringBuilder(25);
        Assert.assertEquals("Empty", "", sb.build(" "));
        Assert.assertEquals("String", "abc", sb.build(" abc"));
        Assert.assertEquals("String with blanks", "abc  abc", sb
                .build(" abc  abc"));
        Assert.assertEquals("String with trailing", "abc   ", sb
                .build("   abc   "));
    }

    /**
     * Ensure normal comments are completely removed
     */
    @Test
    public void testNormalComments() throws Exception
    {
        MySQLOperationStringBuilder sb = new MySQLOperationStringBuilder(100);
        Assert.assertEquals("Empty", "", sb.build("/**/"));
        Assert.assertEquals("String", "abc", sb.build("/*abc*/abc"));
        Assert.assertEquals("String", "abc", sb.build("/*def*/abc/*def*/"));
        Assert.assertEquals("String", "abc", sb.build(" /*def*/ /*def*/ abc"));
        Assert.assertEquals("String", "abc", sb.build("abc/*def*/"));
        Assert.assertEquals("String", "abcghi", sb.build("abc/*def*/ghi"));
        Assert.assertEquals("String", "abc ghi", sb.build("abc/*def*/ ghi"));
        Assert
                .assertEquals(
                        "String",
                        "delete  from foo where id=1",
                        sb
                                .build("/* comment */ delete /* comment */ from foo where id=1"));

    }

    /**
     * Ensure bang comment delimiters are removed.
     */
    @Test
    public void testBangComments() throws Exception
    {
        MySQLOperationStringBuilder sb = new MySQLOperationStringBuilder(100);
        Assert.assertEquals("Empty", "", sb.build("/*!12345*/"));
        Assert.assertEquals("String", "abc", sb.build("/*!23456 abc*/"));
        Assert.assertEquals("String", "abc  def  ghi", sb
                .build("abc /*!23456 def */ ghi"));
        Assert.assertEquals("String", "CREATE DATABASE  IF NOT EXISTS `foo`  "
                + "DEFAULT CHARACTER SET latin1 ", sb
                .build("CREATE DATABASE /*!32312 IF NOT EXISTS*/ `foo` "
                        + "/*!40100 DEFAULT CHARACTER SET latin1 */"));
    }
    
    /**
     * Ensure we always truncate at the expected number of characters. 
     */
    @Test
    public void testTruncation() throws Exception
    {
        MySQLOperationStringBuilder sb = new MySQLOperationStringBuilder(10);
        Assert.assertEquals("Simple truncation", "123456789*", sb.build("123456789*1"));
        Assert.assertEquals("Simple truncation", "123456789*", sb.build(" 1234/*comment*/56789*1"));
        Assert.assertEquals("Simple truncation", "123456789*", sb.build(" 1234/*!9999956*/789*1"));
    }
}