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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * This class fetches and presents online help text. Help is stored in text
 * files that are located in a help directory. Help operates as a separate
 * subsystem with a factory method to fetch manager instances. Help output
 * defaults to System.out.
 */
public class HelpManager
{
    // Location of help files.
    private static File   helpDir;

    // Default name if no topic is specified.
    private static String DEFAULT   = "default";

    // Message to print if topic is not found.
    private static String NOT_FOUND = "not_found.hlp";

    // File type for help files.
    private static String FILE_TYPE = ".hlp";

    private PrintWriter   out       = new PrintWriter(System.out);

    // Private constructor.
    private HelpManager()
    {
    }

    /**
     * Initialize help. This must be called before fetching a help manager.
     * 
     * @param helpDir Location of help files.
     * @throws HelpException Thrown if help directory is invalid
     */
    public static void initialize(File helpDir) throws HelpException
    {
        if (helpDir.isDirectory() && helpDir.exists())
            HelpManager.helpDir = helpDir;
        else
            throw new HelpException("Help directory is missing or invalid: "
                    + helpDir.getAbsolutePath(), null);
    }

    /**
     * Return a help manager instance.
     */
    public static HelpManager getInstance()
    {
        return new HelpManager();
    }

    /**
     * Set the print output writer.
     */
    public void setWriter(Writer writer)
    {
        out = new PrintWriter(writer);
    }

    /**
     * Display help for a topic. The name will be lower-cased and extended with
     * the help file suffix, then displayed.
     * 
     * @param name Help topic name or null to get the default value
     * @return True if help file was successfully displayed, otherwise false.
     */
    public boolean displayTopic(String name)
    {
        // Construct the help name.
        String topic = null;
        if (name == null)
            topic = DEFAULT + FILE_TYPE;
        else
            topic = name.toLowerCase() + FILE_TYPE;

        // Fetch the file.
        File helpFile = new File(helpDir, topic);
        return display(helpFile);
    }

    /**
     * Display help for a topic. The topic consists of one or more strings. To
     * locate the corresponding file, we lower case the strings, connect them by
     * "_" characters, and add the help file suffix.
     * 
     * @param names An array of names used to construct the topic name
     * @return True if help file was successfully displayed, otherwise false.
     */
    public boolean displayTopicFromNames(String[] names)
    {
        // Construct the help name.
        if (names.length == 0)
            return displayTopic(DEFAULT);
        else
        {
            // Construct the help file name from the arrays.
            StringBuffer helpName = new StringBuffer();
            for (String name : names)
            {
                if (helpName.length() > 0)
                    helpName.append("_");
                helpName.append(name);
            }
            return displayTopic(helpName.toString());
        }
    }

    /**
     * Displays the requested help file if it is found. If not, we first try to
     * display the default not found message. If that is not available, we
     * display a default message.
     * 
     * @param helpFile Requested help file.
     * @return True if found and properly displayed
     */
    protected boolean display(File helpFile) throws HelpException
    {
        if (helpFile.exists())
        {
            loadAndWrite(helpFile);
            return true;
        }
        else
        {
            File notFound = new File(helpDir, NOT_FOUND);
            if (notFound.exists())
                loadAndWrite(notFound);
            else
                this.out.println("Topic not found");
            return false;
        }
    }

    /**
     * Copy help file contents to the current output.
     */
    protected void loadAndWrite(File file) throws HelpException
    {
        FileReader fr = null;
        try
        {
            fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null)
            {
                out.println(line);
            }
            out.flush();
        }
        catch (IOException e)
        {

        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }
}