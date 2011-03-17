/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Csaba Simon
 * Contributor(s): Gilles Rayrat
 */

package com.continuent.tungsten.commons.mysql;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

/**
 * Tests various methods of the Utils class.
 * 
 * @author <a href="mailto:csaba.simon@continuent.com">Csaba Simon</a>
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class UtilsTestCase extends TestCase
{
    /**
     * Test generateRandomString() method.
     */
    public void testGenerateRandomString()
    {
        assertEquals(7, Utils.generateRandomString(7).length());
    }

    /**
     * Test the removeQuotes() method.
     */
    public void testRemoveQuotes()
    {
        assertEquals(null, Utils.removeQuotes(null));
        assertEquals("", Utils.removeQuotes(""));
        assertEquals("", Utils.removeQuotes("   "));
        assertEquals("a", Utils.removeQuotes("a"));
        assertEquals("one", Utils.removeQuotes("    \u2018one\' "));
        assertEquals("two", Utils.removeQuotes("`two\u2019 "));
        assertEquals("three", Utils.removeQuotes("\u201Cthree\u00B4"));
        assertEquals("four", Utils.removeQuotes("\"four\u201D\""));
        assertEquals("five", Utils
                .removeQuotes("\"\'\u0060\u00B4five\u00B4\u0060\'\""));
    }

    public void testReplaceParametersWithQuestionMarks()
    {
        assertEquals(null, Utils.replaceParametersWithQuestionMarks(null));
        assertEquals("", Utils.replaceParametersWithQuestionMarks(""));
        assertEquals(
                "select * from t where a = ?::int4",
                Utils
                        .replaceParametersWithQuestionMarks("select * from t where a = $1::int4"));
        assertEquals(
                "select * from t where a = ?::int4 and b = ?::int8",
                Utils
                        .replaceParametersWithQuestionMarks("select * from t where a = $1::int4 and b = $2::int8"));
        assertEquals(
                "select * from func(?::int4)",
                Utils
                        .replaceParametersWithQuestionMarks("select * from func($1::int4)"));

    }

    public void testIsAuthorizedIP()
    {
        // nulls and empties
        assertFalse(Utils.isAuthorizedIP(null, null));
        assertTrue(Utils.isAuthorizedIP("192.168.0.1", null));
        assertFalse(Utils.isAuthorizedIP(null, new ArrayList<String>()));
        assertFalse(Utils
                .isAuthorizedIP("192.168.0.1", new ArrayList<String>()));
        List<String> authorizedIPs = new ArrayList<String>();
        authorizedIPs.add("192.168.0.0/24");
        // bad formats
        assertFalse(Utils.isAuthorizedIP("malformed IP", authorizedIPs));
        assertFalse(Utils.isAuthorizedIP("192.168.1.2.3", authorizedIPs));
        // regular negative
        assertFalse(Utils.isAuthorizedIP("192.168.1.1", authorizedIPs));
        // positive
        assertTrue(Utils.isAuthorizedIP("192.168.0.1", authorizedIPs));
        // multiple authorized IPs
        authorizedIPs.add("192.168.0.1/24");
        assertFalse(Utils.isAuthorizedIP("192.169.1.1", authorizedIPs));
        assertTrue(Utils.isAuthorizedIP("192.168.0.123", authorizedIPs));

        // single address
        authorizedIPs.clear();
        authorizedIPs.add("1.2.3.4/32");
        assertFalse(Utils.isAuthorizedIP("1.2.3.5", authorizedIPs));
        assertTrue(Utils.isAuthorizedIP("1.2.3.4", authorizedIPs));

        // Wide nets
        authorizedIPs.clear();
        authorizedIPs.add("123.234.0.0/16");
        assertFalse(Utils.isAuthorizedIP("123.1.2.3", authorizedIPs));
        assertTrue(Utils.isAuthorizedIP("123.234.2.3", authorizedIPs));
        authorizedIPs.clear();
        authorizedIPs.add("123.0.0.0/8");
        assertFalse(Utils.isAuthorizedIP("124.1.2.3", authorizedIPs));
        assertTrue(Utils.isAuthorizedIP("123.234.2.3", authorizedIPs));
    }
}
