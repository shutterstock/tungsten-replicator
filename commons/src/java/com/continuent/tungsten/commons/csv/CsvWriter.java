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
 * Writes CSV output.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class CsvWriter
{
    // Properties.
    private char                 separator    = ',';
    private boolean              writeHeaders = true;
    private boolean              quoted       = false;
    private char                 quoteChar    = '"';

    // State.
    private Map<String, Integer> names        = new HashMap<String, Integer>();
    private List<String>         row;
    private BufferedWriter       writer;
    private int                  rowCount     = 0;
    private int                  colCount     = 0;

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
        this.writer = writer;
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

    /** Returns true if values will be enclosed by a quote character. */
    public synchronized boolean isQuoted()
    {
        return quoted;
    }

    /** Set to true to enable quoting. */
    public synchronized void setQuoted(boolean quoted)
    {
        this.quoted = quoted;
    }

    /** Returns the quote character. */
    public synchronized char getQuoteChar()
    {
        return quoteChar;
    }

    /** Sets the quote character. */
    public synchronized void setQuoteChar(char quoteChar)
    {
        this.quoteChar = quoteChar;
    }

    /**
     * Returns the current count of rows written.
     */
    public int getRowCount()
    {
        return rowCount;
    }

    /**
     * Get the underlying writer.
     */
    public Writer getWriter()
    {
        return writer;
    }

    /** If true, write headers. */
    public synchronized boolean isWriteHeaders()
    {
        return writeHeaders;
    }

    /** Set to true to write headers. */
    public synchronized void setWriteHeaders(boolean writeHeaders)
    {
        this.writeHeaders = writeHeaders;
    }

    /**
     * Add a column name. Columns are indexed 1,2,3,...,N in the order added.
     * You must add all names before writing the first row.
     * 
     * @param name Column name
     * @throws CsvException Thrown
     */
    public void addColumnName(String name) throws CsvException
    {
        if (rowCount > 0)
        {
            throw new CsvException(
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
     * @throws CsvException Thrown if there is an inconsistency like too many
     *             columns
     * @throws IOException Thrown due to a write error
     */
    public void write() throws CsvException, IOException
    {
        // Write headers if we are at top of file and header write
        // is enabled.
        if (rowCount == 0 && writeHeaders)
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
                throw new CsvException("Attempt to write partial row: row="
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
     * @throws CsvException Thrown on an I/O failure
     */
    public void flush() throws IOException, CsvException
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
     * @throws CsvException Thrown if client attempts to write same column value
     *             twice or the row is not wide enough
     */
    public void put(int index, String value) throws CsvException
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

        // Check for invalid index.
        if (index < 1 || index > row.size())
        {
            throw new CsvException("Attempt to to invalid column index: index="
                    + index + " value=" + value + " row size=" + row.size());
        }

        // Check for a double write to same column. This is a safety violation.
        int arrayIndex = index - 1;
        if (row.get(arrayIndex) != null)
        {
            throw new CsvException(
                    "Attempt to write value twice to same row: index=" + index
                            + " old value=" + row.get(arrayIndex)
                            + " new value=" + value);
        }

        // Set the column value.
        if (quoted)
        {
            value = addQuotes(value);
        }
        row.set(arrayIndex, value);
        colCount++;
    }

    /**
     * Writes value to key in current row.
     */
    public void put(String key, String value) throws CsvException
    {
        int index = names.get(key);
        put(index, value);
    }

    // Utility routine to escape string contents and enclose in
    // quotes.
    private String addQuotes(String base)
    {
        StringBuffer sb = new StringBuffer();
        sb.append(quoteChar);
        for (int i = 0; i < base.length(); i++)
        {
            char next = base.charAt(i);
            if (next == quoteChar)
                sb.append('\\').append(quoteChar);
            else if (next == '\\')
                sb.append('\\').append('\\');
            else
                sb.append(next);
        }
        sb.append(quoteChar);
        return sb.toString();
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
