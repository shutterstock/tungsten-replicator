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
 * Contributor(s):
 */

package com.continuent.tungsten.commons.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.junit.Assert;
import org.junit.Test;

/**
 * Implements a basic unit test of CSV input and output.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class CsvTest
{
    /**
     * Verify that we can write a file to output and read it back in.
     */
    @Test
    public void testOutputInput() throws Exception
    {
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);

        // Write values.
        csvWriter.addColumnName("a");
        csvWriter.addColumnName("bb");
        csvWriter.addColumnName("ccc");
        csvWriter.put("a", "r1a");
        csvWriter.put("bb", "r1b");
        csvWriter.put("ccc", "r1c");
        csvWriter.write();
        csvWriter.put("a", "r2a");
        csvWriter.put("bb", "r2b");
        csvWriter.put("ccc", "r2c");
        csvWriter.flush();
        String csv = sw.toString();

        // Read values back in again and validate.
        StringReader sr = new StringReader(csv);
        CsvReader csvReader = new CsvReader(sr);

        // Validate names.
        Assert.assertTrue("First read succeeded", csvReader.next());
        Assert.assertEquals("size of names", 3, csvReader.getNames().size());
        Assert.assertEquals("a", csvReader.getNames().get(0));
        Assert.assertEquals("bb", csvReader.getNames().get(1));
        Assert.assertEquals("ccc", csvReader.getNames().get(2));

        // Check row 1 values using index and names.
        Assert.assertEquals("r1 val1", "r1a", csvReader.getString(1));
        Assert.assertEquals("r1 val1", "r1a", csvReader.getString("a"));

        Assert.assertEquals("r1 val2", "r1b", csvReader.getString(2));
        Assert.assertEquals("r1 val2", "r1b", csvReader.getString("bb"));

        Assert.assertEquals("r1 val1", "r1c", csvReader.getString(3));
        Assert.assertEquals("r1 val1", "r1c", csvReader.getString("ccc"));

        // Check row 2 values.
        Assert.assertTrue("Second read succeeded", csvReader.next());
        Assert.assertEquals("r1 val1", "r2a", csvReader.getString(1));
        Assert.assertEquals("r1 val1", "r2a", csvReader.getString("a"));

        Assert.assertEquals("r1 val2", "r2b", csvReader.getString(2));
        Assert.assertEquals("r1 val2", "r2b", csvReader.getString("bb"));

        Assert.assertEquals("r1 val1", "r2c", csvReader.getString(3));
        Assert.assertEquals("r1 val1", "r2c", csvReader.getString("ccc"));

        // Ensure we are done.
        Assert.assertFalse("Third read failed", csvReader.next());
    }

    /**
     * Verify that a CsvException results if the client tries to add a new
     * column name after writing the first row.
     */
    @Test
    public void testSetHeaderAfterWrite() throws Exception
    {
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);

        csvWriter.addColumnName("a");
        csvWriter.put("a", "r1a");
        csvWriter.write();

        try
        {
            csvWriter.addColumnName("bb");
            throw new Exception("Can add column after writing!");
        }
        catch (CsvException e)
        {
            // Expected.
        }
    }

    /**
     * Verify that a CsvException results if the client issues a write while we
     * have an incomplete row.
     */
    @Test
    public void testWriteIncompleteRow() throws Exception
    {
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);

        csvWriter.addColumnName("a");
        csvWriter.addColumnName("bb");
        csvWriter.put("a", "r1a");
        try
        {
            csvWriter.write();
            throw new Exception("Can write partial row!");
        }
        catch (CsvException e)
        {
            // Expected.
        }
        try
        {
            csvWriter.write();
            throw new Exception("Can flush partial row!");
        }
        catch (CsvException e)
        {
            // Expected.
        }

        // Verify that we can write as well as flush once we add the extra row.
        csvWriter.put("bb", "r1b");
        csvWriter.write();
        csvWriter.flush();
    }

    /**
     * Verify that a CsvException results if the client issues a write past the
     * end of the row.
     */
    @Test
    public void testWriteExtraColumn() throws Exception
    {
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);

        csvWriter.addColumnName("a");
        csvWriter.put(1, "good value");
        try
        {
            csvWriter.put(2, "bad value");
            throw new Exception("Can write extra column!");
        }
        catch (CsvException e)
        {
            // Expected.
        }
    }

    /**
     * Verify that if a client attempts to read a non-existent column an
     * IOException results.
     */
    @Test
    public void testReadNonExistent() throws Exception
    {
        // Load a CSV file.
        String[] colNames = {"a", "b", "c"};
        String csv = this.createCsvFile(colNames, 10);
        StringReader sr = new StringReader(csv);
        CsvReader csvReader = new CsvReader(sr);

        // Validate reading existing fields.
        Assert.assertTrue("First read succeeded", csvReader.next());
        Assert.assertEquals("size of names", 3, csvReader.getNames().size());
        Assert.assertEquals("r1_c1", csvReader.getString("a"));
        Assert.assertEquals("r1_c2", csvReader.getString("b"));
        Assert.assertEquals("r1_c3", csvReader.getString("c"));

        // Read non-existing column name.
        try
        {
            csvReader.getString("d");
            throw new Exception("Able to read invalid column name");
        }
        catch (CsvException e)
        {
            // Expected.
        }

        // Read non-existing column index.
        try
        {
            csvReader.getString(4);
            throw new Exception("Able to read invalid column index");
        }
        catch (CsvException e)
        {
            // Expected.
        }
    }

    // Create a CSV file with no extra separators.
    private String createCsvFile(String[] colNames, int rows)
            throws IOException
    {
        // Set up output.
        StringWriter sw = new StringWriter();
        BufferedWriter bw = new BufferedWriter(sw);

        // Write headers.
        for (int c = 1; c <= colNames.length; c++)
        {
            if (c > 1)
                bw.append(",");
            bw.append(colNames[c - 1]);
        }

        // Write each row.
        for (int r = 1; r <= rows; r++)
        {
            bw.newLine();
            for (int c = 1; c <= colNames.length; c++)
            {
                if (c > 1)
                    bw.append(",");
                String value = "r" + r + "_c" + c;
                bw.append(value);
            }
        }

        // Flush output and return value.
        bw.flush();
        return sw.toString();
    }
}