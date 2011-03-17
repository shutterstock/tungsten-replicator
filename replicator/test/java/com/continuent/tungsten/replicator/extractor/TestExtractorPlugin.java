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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */


package com.continuent.tungsten.replicator.extractor;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginLoader;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * 
 * This class checks extractor loading using the DummyExtractor. 
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class TestExtractorPlugin extends TestCase
{

    static Logger logger = null;
    public void setUp() throws Exception
    {
        if (logger == null)
            logger = Logger.getLogger(TestExtractorPlugin.class);
    }
    
   /*
    * Test that dummy extractor works like expected, 
    */
    public void testExtractorBasic() throws Exception
    {
        
        RawExtractor extractor = (RawExtractor) PluginLoader.load(DummyExtractor.class.getName());
        
        extractor.configure(null);
        extractor.prepare(null);
        
        DBMSEvent event = extractor.extract();
        Assert.assertEquals(event.getEventId(), "0");
        event = extractor.extract();
        Assert.assertEquals(event.getEventId(), "1");
        
        extractor.setLastEventId("0");
        event = extractor.extract();
        Assert.assertEquals(event.getEventId(), "1");
        
        extractor.setLastEventId(null);
        event = extractor.extract();
        Assert.assertEquals(event.getEventId(), "0");
        
        for (Integer i = 1; i < 5; ++i)
        {
            event = extractor.extract();
            Assert.assertEquals(event.getEventId(), i.toString());
        }
        
        event = extractor.extract("0");
        Assert.assertEquals(event.getEventId(), "0");

        event = extractor.extract("4");
        Assert.assertEquals(event.getEventId(), "4");

        event = extractor.extract("5");
        Assert.assertEquals(event, null);

        
        
        extractor.release(null);
        
    }
    
    /*
     * Test test that event ID calls work as expected 
     */
     public void testExtractorEventID() throws Exception
     {
         
         RawExtractor extractor = (RawExtractor) PluginLoader.load(DummyExtractor.class.getName());
         
         extractor.configure(null);
         extractor.prepare(null);

         DBMSEvent event = extractor.extract();
         String currentEventId = extractor.getCurrentResourceEventId();
         Assert.assertEquals(event.getEventId(), currentEventId);
         
         event = extractor.extract();
         Assert.assertTrue(event.getEventId().compareTo(currentEventId) > 0);

         currentEventId = extractor.getCurrentResourceEventId();
         Assert.assertTrue(event.getEventId().compareTo(currentEventId) == 0);

         extractor.release(null);
     }
    
}
