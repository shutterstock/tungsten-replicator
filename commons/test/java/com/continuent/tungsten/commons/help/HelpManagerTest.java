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

package com.continuent.tungsten.commons.help;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;

import org.junit.Assert;
import org.junit.Test;

/**
 * Implements a unit test of help manager functions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class HelpManagerTest
{
    /**
     * Verify that help subsystem initializes when help directory is valid.
     */
    @Test
    public void testHelpInitialization() throws Exception
    {
        File helpDir = createHelpDir("helpTest1");
        HelpManager.initialize(helpDir);
        HelpManager mgr = HelpManager.getInstance();
        Assert.assertNotNull("Help manager is not null", mgr);
        helpDir.delete();
    }

    /**
     * Verify that an existing file can be found and displayed.
     */
    @Test
    public void testDisplayExisting() throws Exception
    {
        // Set up help directory and a content file.
        File helpDir = createHelpDir("helpTest2");
        HelpManager.initialize(helpDir);
        String helpContent = "help content";
        File contentFile = fillContentFile(helpDir, "help_example.hlp",
                helpContent);

        // Display the file using a simple name.
        HelpManager mgr = HelpManager.getInstance();
        StringWriter sw = new StringWriter();
        mgr.setWriter(sw);
        mgr.displayTopic("help_example");
        assertMinimumStringMatch("Content from simple topic", helpContent, sw
                .toString());

        // Display the file using an array.
        StringWriter sw2 = new StringWriter();
        mgr.setWriter(sw2);
        String[] names = new String[]{"help", "example"};
        mgr.displayTopicFromNames(names);
        assertMinimumStringMatch("Content from simple topic", helpContent, sw2
                .toString());

        // Clean up.
        contentFile.delete();
        helpDir.delete();
    }

    /**
     * Verify that when files are not found we display an appropriate message.
     */
    @Test
    public void testNotFound() throws Exception
    {
        // Set up help directory and a content file.
        File helpDir = createHelpDir("helpTest3");
        HelpManager.initialize(helpDir);

        // Demonstrate we cannot find the topic.
        HelpManager mgr = HelpManager.getInstance();
        boolean found = mgr.displayTopic("invalid_topic");
        Assert.assertFalse("Can't find the topic", found);

        // Add a not found file and do it again.
        String notFoundContent = "not found!!!";
        File contentFile = fillContentFile(helpDir, "not_found.hlp",
                notFoundContent);

        // Display the file using an array.
        StringWriter sw2 = new StringWriter();
        mgr.setWriter(sw2);
        found = mgr.displayTopic("invalid_topic");
        Assert.assertFalse("Can't find the topic", found);
        assertMinimumStringMatch("Content from simple topic", notFoundContent,
                sw2.toString());

        // Clean up.
        contentFile.delete();
        helpDir.delete();
    }

    // Create the help directory in a suitable temp location.
    private File createHelpDir(String name) throws Exception
    {
        // Find a parent directory.
        File temp = new File("/tmp");
        if (!temp.exists())
        {
            temp = new File(".");
        }

        // Create temp directory.
        File helpDir = new File(temp, name);
        if (!helpDir.exists())
        {
            helpDir.mkdir();
        }
        if (!helpDir.exists())
            throw new Exception("Unable to create help directory: "
                    + helpDir.getAbsolutePath());

        return helpDir;
    }

    // Write help file content.
    private File fillContentFile(File dir, String fileName, String content)
            throws IOException
    {
        File helpFile = new File(dir, fileName);
        FileWriter fw = new FileWriter(helpFile);
        fw.write(content);
        fw.close();
        return helpFile;
    }

    // Assert that strings match to the expected value length. This is necessary
    // due to extra CR/LF that appear on output values
    private void assertMinimumStringMatch(String msg, String expected,
            String actual) throws Exception
    {
        Assert.assertEquals("Content from simple topic", expected, actual
                .substring(0, expected.length()));
    }
}
