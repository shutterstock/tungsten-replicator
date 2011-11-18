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
 * Contributor(s): Linas Virbalas
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
 * Writes CSV output. This class implements CSV formatting roughly as described
 * in RFC4180 (http://tools.ietf.org/html/rfc4180) with practical alterations to
 * match specify DBMS implementations.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class CsvWriter
{
    // Properties.
    private char                 separator       = ',';
    private boolean              writeHeaders    = true;
    private boolean              quoted          = false;
    private NullPolicy           nullPolicy      = NullPolicy.skip;
    private String               nullValue       = null;
    private char                 quoteChar       = '"';
    private char                 escapeChar      = '\\';
    private String               escapedChars    = "";
    private String               suppressedChars = "";
    private String               rowId           = null;

    // State.
    private Map<String, Integer> names           = new HashMap<String, Integer>();
    private List<String>         row;
    private BufferedWriter       writer;
    private int                  rowCount        = 0;
    private int                  colCount        = 0;

    // Enum and table to describe disposition of specific characters.
    enum Disposition
    {
        escape, suppress
    }

    private Map<Character, Disposition> disposition;

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

    /** Returns the policy for handling null values. */
    public synchronized NullPolicy getNullPolicy()
    {
        return nullPolicy;
    }

    /** Sets the policy for handling null values. */
    public synchronized void setNullPolicy(NullPolicy nullPolicy)
    {
        this.nullPolicy = nullPolicy;
    }

    /** Gets the null value identifier string. */
    public synchronized String getNullValue()
    {
        return nullValue;
    }

    /**
     * Sets the null value identifier string. This applies only when null policy
     * is NullPolicy.nullValue.
     */
    public synchronized void setNullValue(String nullValue)
    {
        this.nullValue = nullValue;
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
     * Sets character used to escape quotes and other escaped characters.
     * 
     * @see #setQuoteChar(char)
     */
    public synchronized void setEscapeChar(char quoteEscapeChar)
    {
        this.escapeChar = quoteEscapeChar;
    }

    /** Returns the escape character. */
    public synchronized char getEscapeChar()
    {
        return escapeChar;
    }

    /**
     * Returns a string of characters that must be preceded by escape character.
     */
    public synchronized String getEscapedChars()
    {
        return escapedChars;
    }

    /**
     * Defines zero or more characters that must be preceded by escape
     * character.
     */
    public synchronized void setEscapedChars(String escapedChars)
    {
        if (escapedChars == null)
            this.escapedChars = "";
        else
            this.escapedChars = escapedChars;
    }

    /**
     * Returns a string of characters that are suppressed in CSV output.
     */
    public synchronized String getSuppressedChars()
    {
        return suppressedChars;
    }

    /**
     * Sets characters to be suppressed in CSV output.
     */
    public synchronized void setSuppressedChars(String suppressedChars)
    {
        if (suppressedChars == null)
            this.suppressedChars = "";
        else
            this.suppressedChars = suppressedChars;
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
     * Add a row id name. Row IDs are a numeric counter at the end of the rows
     * to make loading processes easier for data warehouses. They automatically
     * join the index as the last column.
     * 
     * @param name Row ID name
     * @throws CsvException Thrown if the row ID has already been set.
     */
    public void addRowIdName(String name) throws CsvException
    {
        if (rowCount > 0)
        {
            throw new CsvException(
                    "Attempt to add row ID after writing one or more rows");
        }
        else if (rowId != null)
        {
            throw new CsvException("Attempt to add row ID twice");
        }
        this.rowId = name;
    }

    /**
     * Return names in column order.
     */
    public List<String> getNames()
    {
        // Create null-filled array. The array differs by one according
        // to whether we use row IDs or not.
        int size = (rowId == null) ? names.size() : names.size() + 1;
        List<String> nameList = new ArrayList<String>(size);
        for (int i = 0; i < size; i++)
            nameList.add(null);

        // Add names to correct positions in array.
        for (String name : names.keySet())
        {
            int index = names.get(name);
            nameList.set(index - 1, name);
        }

        // Add rowId if we are using it.
        if (rowId != null)
            nameList.set(names.size(), rowId);

        return nameList;
    }

    /**
     * Return the number of columns.
     */
    public int getWidth()
    {
        int base = names.size();
        if (rowId == null)
            return base;
        else
            return base + 1;
    }

    /**
     * Writes current row, including headers if we are on the first row.
     * 
     * @throws CsvException Thrown if there is an inconsistency like too many
     *             columns
     * @throws IOException Thrown due to a write error
     */
    public CsvWriter write() throws CsvException, IOException
    {
        // At the top of the file optionally write headers and set the row
        // ID name.
        if (rowCount == 0 && writeHeaders)
        {
            if (writeHeaders)
            {
                writeRow(getNames());
                rowCount++;
            }
        }

        // If we have a pending row, write it now.
        if (row != null)
        {
            // Add the row count value to last column if row IDs are enabled.
            if (rowId != null)
            {
                put(row.size(), new Integer(rowCount + 1).toString());
            }

            // Check for writing too few columns.
            if (colCount < names.size())
            {
                throw new CsvException("Attempt to write partial row: row="
                        + (rowCount + 1) + " columns required=" + names.size()
                        + " columns written=" + colCount);
            }

            // Write the row.
            writeRow(row);
            row = null;
            colCount = 0;
            rowCount++;
        }

        return this;
    }

    /**
     * Forces a write of any pending row(s) and flushes data on writer.
     * 
     * @throws CsvException Thrown on an I/O failure
     */
    public CsvWriter flush() throws IOException, CsvException
    {
        write();
        writer.flush();
        return this;
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
    public CsvWriter put(int index, String value) throws CsvException
    {
        // Initialize the character disposition table if necessary.
        if (disposition == null)
        {
            disposition = new HashMap<Character, Disposition>(256);
            for (char c : escapedChars.toCharArray())
            {
                disposition.put(c, Disposition.escape);
            }
            for (char c : suppressedChars.toCharArray())
            {
                disposition.put(c, Disposition.suppress);
            }
        }

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
            throw new CsvException(
                    "Attempt to write to invalid column index: index=" + index
                            + " value=" + value + " row size=" + row.size());
        }

        // Check for a double write to same column. This is a safety violation.
        int arrayIndex = index - 1;
        if (row.get(arrayIndex) != null)
        {
            throw new CsvException(
                    "Attempt to write value twice to same row: index="
                            + index
                            + " old value="
                            + row.get(arrayIndex)
                            + " new value="
                            + value
                            + " (does table have a PK and is it single-column?)");
        }

        // Set the column value.
        if (value == null)
        {
            // Nulls are handled according to the null value policy.
            if (this.nullPolicy == NullPolicy.emptyString)
                value = addQuotes("");
            else if (nullPolicy == NullPolicy.skip)
                value = null;
            else
                value = nullValue;
        }
        else if (quoted)
        {
            value = addQuotes(value);
        }
        row.set(arrayIndex, value);
        colCount++;

        return this;
    }

    /**
     * Writes value to key in current row.
     */
    public CsvWriter put(String key, String value) throws CsvException
    {
        int index = names.get(key);
        return put(index, value);
    }

    // Utility routine to escape string contents and enclose in
    // quotes.
    private String addQuotes(String base)
    {
        StringBuffer sb = new StringBuffer();
        sb.append(quoteChar);
        for (int i = 0; i < base.length(); i++)
        {
            // Fetch character and look up its disposition.
            char next = base.charAt(i);
            Disposition disp = disposition.get(next);

            // Emit the character according to CSV formatting rules.
            if (next == quoteChar)
            {
                // Escape any quote character.
                sb.append(escapeChar).append(quoteChar);
            }
            else if (disp == Disposition.escape)
            {
                // Prefix an escape character.
                sb.append(escapeChar).append(next);
            }
            else if (disp == Disposition.suppress)
            {
                // Drop the character.
                continue;
            }
            else
            {
                // If all else fails, emit the character as is.
                sb.append(next);
            }
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
            String value = row.get(i);
            if (value != null)
                writer.append(row.get(i));
        }
        writer.newLine();
    }
}