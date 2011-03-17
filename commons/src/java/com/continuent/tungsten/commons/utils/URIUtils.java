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
 */

package com.continuent.tungsten.commons.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Vector;

import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * This class defines a URLUtils
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class URIUtils
{
    private static final String   START_OF_ADDITIONAL_ARG = "&";
    private static final int      KEY_INDEX               = 0;
    private static final int      VAL_INDEX               = 1;
    private static final String   EQUALS                  = "=";
    private static final String[] validKeys               = {
            "com.continuent.tungsten.commons.config.routerLatency", "sessionId", "qos"       };

    public static TungstenProperties parse(String url)
            throws URISyntaxException
    {
        URI uri = null;

        // Let the URI constructor do some of the heavy lifting
        uri = new URI(url);
        return parseQuery(uri.getQuery());

    }

    public static TungstenProperties parseQuery(String query)
            throws URISyntaxException
    {
        TungstenProperties args = new TungstenProperties();

        if (query == null)
        {
            return args;
        }

        String[] argSets = query.split(START_OF_ADDITIONAL_ARG);

        for (String argSet : argSets)
        {
            String[] keyVal = argSet.split(EQUALS);
            if (keyVal.length != 2)
            {
                throw new URISyntaxException(String.format(
                        "Malformed URI, expected key=value, got ='%s'",
                        arrayToString(keyVal)), argSet);
            }
            else
            {
                if (keyVal[KEY_INDEX].length() == 0)
                {
                    throw new URISyntaxException(
                            String
                                    .format("Malformed URI, expected key=value, got empty key"),
                            argSet);
                }
                args.setString(keyVal[KEY_INDEX].trim(), keyVal[VAL_INDEX].trim());
            }
        }

        return args;
    }

    private static String arrayToString(String[] array)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        int valCount = 0;
        for (String val : array)
        {
            if (valCount++ > 0)
            {
                builder.append(", ");
            }
            builder.append(val);

        }
        builder.append("}");

        return builder.toString();
    }

    public static void checkKeys(TungstenProperties props) throws URISyntaxException
    {
        Vector<String> invalidKeys = new Vector<String>();
        for (String key : props.keyNames())
        {
            boolean wasFound = false;
            for (String validKey : validKeys)
            {
                if (key.equals(validKey))
                {
                    wasFound = true;
                }
            }
            if (!wasFound)
            {
                invalidKeys.add(key);
            }
        }
        
        if (invalidKeys.size() > 0)
        {
            throw new URISyntaxException(String.format(
                    "Found one or more invalid keys. Invalid values are: '%s'\n" +
                    "Valid values are: %s", invalidKeys.toString(), arrayToString(validKeys)), 
                    props.toString());
                    
        }
        
    }
}
