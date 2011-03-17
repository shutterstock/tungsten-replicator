
package com.continuent.tungsten.commons.utils;

import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;

import org.junit.Test;

import com.continuent.tungsten.commons.config.TungstenProperties;

public class TestURIUtils
{

    @Test
    public void testParseQuery()
    {
        String malformedQueries[] = {"?&&?", "?xx,", "?key=&", "?",
                "?key1=    value, key2=",
                "xyz?qos=RW_STRICT&com.continuent.tungsten.commons.config.routerLatency=&sessionId=ABCLLDDDDOEE",
                "mydb?qos=RW_SESSION&sessionID=234567AAAA"};

        String validQueries[] = {"qos=value",

        "sessionId=value1?qos=value2",
                "//thisisatest/x?qos=value1&sessionId=value2%20&com.continuent.tungsten.commons.config.routerLatency=%20%20value3",
                "//default/xyz?qos=RW_STRICT&com.continuent.tungsten.commons.config.routerLatency=1234&sessionId=ABCLLDDDDOEE",
                "mydb?qos=RW_SESSION&sessionId=234567AAAA"
               };

        TungstenProperties queryProps = null;

        int malFormedCount = 0;

        for (int i = 0; i < malformedQueries.length; i++)
        {
            try
            {
                System.out.println(String.format("INVALID: Parsing query='%s'",
                        malformedQueries[i]));
                queryProps = URIUtils.parse(malformedQueries[i]);
                URIUtils.checkKeys(queryProps);
                System.out.println(String.format(
                        "INVALID: Parsed the following properties from url:%s",
                        queryProps.toString()));
            }
            catch (URISyntaxException m)
            {
                System.out.println(String
                        .format("INVALID: Caught expected exception:%s", m
                                .getMessage()));
                malFormedCount++;
            }
        }

        assertTrue(String.format(
                "INVALID: %d queries should have been invalid. Found %d",
                malformedQueries.length, malFormedCount),
                malFormedCount == malformedQueries.length);

        malFormedCount = 0;

        for (int i = 0; i < validQueries.length; i++)
        {
            try
            {
                System.out.println(String.format("VALID: Parsing query='%s'",
                        validQueries[i]));
                queryProps = URIUtils.parse(validQueries[i]);
                System.out.println(String.format(
                        "VALID: Parsed the following properties from url:%s",
                        queryProps.toString()));
                URIUtils.checkKeys(queryProps);
            }
            catch (URISyntaxException m)
            {
                System.out.println(String
                        .format("VALID: Caught unexpected exception:%s", m
                                .getMessage()));
                malFormedCount++;
            }
        }

        assertTrue(String.format(
                "VALID: %d queries should have been invalid. Found %d", 0,
                malFormedCount), malFormedCount == 0);
    }
    
   
}
