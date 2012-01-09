/**
 * Tungsten Scale-Out Stack
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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.database;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This class generates SQL scripts consisting of multiple parameterized lines
 * read from a file. Each SQL command is a single starting in column 1 with
 * subsequent lines indented. This class reads the raw file once and then
 * quickly generates subsequent versions using the getParameterizedScript()
 * method.
 */
public class SqlScriptGenerator
{
    List<String> rawCommands = new LinkedList<String>();

    public SqlScriptGenerator()
    {
    }

    /**
     * Loads a set of raw commands from input.
     * 
     * @param reader Reader from which to read unparameterized commands
     * @throws IOException Thrown if there is an I/O error during reading
     */
    public void load(Reader reader) throws IOException
    {
        rawCommands = new LinkedList<String>();
        BufferedReader bufferedReader = new BufferedReader(reader);
        StringBuffer nextCommand = new StringBuffer();
        String nextLine;
        while ((nextLine = bufferedReader.readLine()) != null)
        {
            if (nextLine.length() == 0)
            {
                // Ignore empty lines.
                continue;
            }
            else if (nextLine.charAt(0) == '#')
            {
                // Ignore comments.
                continue;
            }
            else if (Character.isWhitespace(nextLine.charAt(0)))
            {
                // Append indented lines to current command 
                // with CR converted to space.
                nextCommand.append(" ").append(nextLine);
            }
            else
            {
                // Anything else is the start of a new line. Dump and
                // parameterize current line, then start a new one.
                if (nextCommand.length() > 0)
                {
                    rawCommands.add(nextCommand.toString());
                }
                nextCommand = new StringBuffer(nextLine);
            }
        }

        // If we have a line pending at the end, don't forget it.
        rawCommands.add(nextCommand.toString());
    }

    /**
     * Returns a command script with parameters assigned.
     * 
     * @param parameters Map containing parameters as name/value pairs
     * @return Ordered list of paramaterized commands
     */
    public List<String> getParameterizedScript(Map<String, String> parameters)
    {
        List<String> commands = new ArrayList<String>(rawCommands.size());
        for (String command : rawCommands)
        {
            for (String key : parameters.keySet())
            {
                String value = parameters.get(key);
                command = command.replace(key, value);
            }
            commands.add(command);
        }
        return commands;
    }
}