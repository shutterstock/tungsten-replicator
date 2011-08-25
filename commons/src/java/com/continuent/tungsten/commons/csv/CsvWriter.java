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

package com.continuent.tungsten.commons.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes CSV format output with appropriate conversions from Java data types.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class CsvWriter
{
    // Properties.
    private char                 separator = ',';

    // State.
    private Map<String, Integer> names     = new HashMap<String, Integer>();
    private List<String>         row;
    private BufferedWriter       writer;
    private int                  rowCount  = 0;
    private int                  colCount  = 0;

    /**
     * Instantiate a new instance with output to provided writer.
     */
    public CsvWriter(Writer writer)
    {
        this(new BufferedWriter(writer));
    }

    /**
     * Instantiate a new instance with output to provided buffered writer. This
     * call allows clients to set buffering parameters themselves.
     */
    public CsvWriter(BufferedWriter writer)
    {
        this.writer = new BufferedWriter(writer);
    }

    /**
     * Sets the separator characters.
     */
    public void setSeparator(char separator)
    {
        this.separator = separator;
    }

    /**
     * Returns separator character.
     */
    public char getSeparator()
    {
        return separator;
    }

    /**
     * Returns the current count of rows written.
     */
    public int getRowCount()
    {
        return rowCount;
    }

    /**
     * Add a column name. Columns are indexed 1,2,3,...,N in the order added.
     * You must add all names before writing the first row.
     * 
     * @param name Column name
     * @throws IOException Thrown
     */
    public void addColumnName(String name) throws IOException
    {
        if (rowCount > 0)
        {
            throw new IOException(
                    "Attempt to add column after writing one or more rows");
        }
        int index = names.size() + 1;
        names.put(name, index);
    }

    /**
     * Return names in column order.
     */
    public List<String> getNames()
    {
        // Create null-filled array.
        List<String> nameList = new ArrayList<String>(names.size());
        for (int i = 0; i < names.size(); i++)
            nameList.add(null);

        // Add names to correct positions in array.
        for (String name : names.keySet())
        {
            int index = names.get(name);
            nameList.set(index - 1, name);
        }
        return nameList;
    }

    /**
     * Return the number of columns.
     */
    public int getWidth()
    {
        return names.size();
    }

    /**
     * Writes current row, including headers if we are on the first row.
     * 
     * @throws IOException
     */
    public void write() throws IOException
    {
        // Write headers if we are at top of file.
        if (rowCount == 0)
        {
            writeRow(getNames());
            rowCount++;
        }

        // If we have a pending row, write it now.
        if (row != null)
        {
            // Check for writing too few columns.
            if (colCount < names.size())
            {
                throw new IOException("Attempt to write partial row: row="
                        + (rowCount + 1) + " columns required=" + names.size()
                        + " columns written=" + colCount);
            }

            writeRow(row);
            row = null;
            colCount = 0;
            rowCount++;
        }
    }

    /**
     * Forces a write of any pending row(s) and flushes data on writer.
     * 
     * @throws IOException Thrown on an I/O failure
     */
    public void flush() throws IOException
    {
        write();
        writer.flush();
    }

    /**
     * Writes value to current row. This is the base value.
     * 
     * @param index Column index where indexes are numbered 1,2,3,...,N with N
     *            being the width of the row in columns
     * @param value String value to write, already escaped if necessary
     * @throws IOException Thrown if client attempts to write same column value
     *             twice
     */
    public void put(int index, String value) throws IOException
    {
        // Start a new row if required and fill columns with null values.
        if (row == null)
        {
            int size = getWidth();
            row = new ArrayList<String>(size);
            for (int i = 0; i < size; i++)
                row.add(null);
            colCount = 0;
        }

        // Check for a double write to same column. This is a safety violation.
        int arrayIndex = index - 1;
        if (row.get(arrayIndex) != null)
        {
            throw new IOException(
                    "Attempt to write value twice to same row: index=" + index
                            + " old value=" + row.get(arrayIndex)
                            + " new value=" + value);
        }

        // Set the column value.
        row.set(arrayIndex, value);
        colCount++;
    }

    /**
     * Writes value to key in current row.
     */
    public void put(String key, String value) throws IOException
    {
        int index = names.get(key);
        put(index, value);
    }

    /**
     * Write contents of a single row, including separator.
     * 
     * @param row
     * @throws IOException
     */
    private void writeRow(List<String> row) throws IOException
    {
        for (int i = 0; i < row.size(); i++)
        {
            if (i > 0)
                writer.append(separator);
            writer.append(row.get(i));
        }
        writer.newLine();
    }
}
