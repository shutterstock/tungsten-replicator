/**
 * Tungsten: An Application Server for uni/cluster.
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

package com.continuent.tungsten.replicator.database;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests SQL name matching using a variety of typical SQL statements.
 * <p/>
 * As currently written, this test includes checks on MySQL schema. It needs to
 * be expanded to work with other database types, such as Drizzle.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestSqlOperationMatcher
{
    private static Logger logger = Logger.getLogger(SqlOperationMatcher.class);

    /**
     * Test unrecognized / junk values.
     */
    @Test
    public void testUnrecognized() throws Exception
    {
        String[] cmds = {"create xxxx database foo", "",
                "   create   DATABASxe  \"foo\"",
                "create `TABLE` `foo` /* hello*/"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found database: " + cmd,
                    SqlOperation.UNRECOGNIZED, sqlName.getObjectType());
            Assert.assertEquals("Found database: " + cmd,
                    SqlOperation.UNRECOGNIZED, sqlName.getOperation());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertNull("Found table: " + cmd, sqlName.getName());
        }
    }

    /**
     * Test basic create database commands.
     */
    @Test
    public void testCreateDb() throws Exception
    {
        String[] cmds = {"create database foo",
                "CREATE DATABASE IF NOT EXISTS foo",
                "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `foo` /*!40100 DEFAULT CHARACTER SET latin1 */",
                "   create   DATABASe  \"foo\"",
                "create database `foo` /* hello*/"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found database: " + cmd, SqlOperation.SCHEMA,
                    sqlName.getObjectType());
            Assert.assertEquals("Found database: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found database: " + cmd, "foo", sqlName
                    .getSchema());
            Assert.assertNull("Found database: " + cmd, sqlName.getName());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }
    }

    /**
     * Test basic drop database commands.
     */
    @Test
    public void testDropDb() throws Exception
    {
        String[] cmds = {"drop database foo", "DROP DATABASE IF EXISTS foo",
                "  droP   DATABASe  \"foo\"", "drop database `foo` /* hello*/"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found database: " + cmd, SqlOperation.SCHEMA,
                    sqlName.getObjectType());
            Assert.assertEquals("Found database: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found database: " + cmd, "foo", sqlName
                    .getSchema());
            Assert.assertNull("Found database: " + cmd, sqlName.getName());
        }
    }

    /**
     * Test create table with and without db name.
     */
    @Test
    public void testCreateTable() throws Exception
    {
        String[] cmds1 = {"create table foo", "CREATE TABLE IF NOT EXISTS foo",
                "   creAtE TEMPORary TabLE \"foo\"",
                "create   table   `foo` /* hello*/"};
        String[] cmds2 = {"create table bar.foo", "CREATE TABLE bar.foo",
                "creAtE TabLE \"bar\".\"foo\"",
                "create temporary  table   `bar`.`foo` /* hello*/"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test create table with and without db name.
     */
    @Test
    public void testDropTable() throws Exception
    {
        String[] cmds1 = {"drop table foo", "DROP TABLE foo",
                "DrOp TabLE \"foo\"",
                "drop temporary  table   `foo` /* hello*/",
                "drop    table  if   exists  foo"};
        String[] cmds2 = {"drop table bar.foo", "DROP TABLE bar.foo",
                "DRop TemporarY TabLE \"bar\".\"foo\"",
                "drop   table   `bar`.`foo` /* hello*/",
                "drop table  if  exists bar.foo"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test insert with and without db name.
     */
    @Test
    public void testInsert() throws Exception
    {
        String[] cmds1 = {"insert into foo values(1)",
                "INSERT INTO foo(id,msg) values(1, 'data')",
                "InSeRt  LOW_PRIORITY  InTo \"foo\" values (1, 'data')",
                "insert  delayed into    `foo` /* hello*/ (one,too) values(1,2)"};
        String[] cmds2 = {"insert into bar.foo values(1)",
                "INSERT INTO bar.foo(id,msg) values(1, 'data')",
                "InSeRt InTo \"bar\".\"foo\" values (1, 'data')",
                "insert   ignore  into    `bar`.`foo` /* hello*/ (one,too) values(1,2)"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.INSERT,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.INSERT,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test replace with and without db name.
     */
    @Test
    public void testReplace() throws Exception
    {
        String[] cmds1 = {"replace into foo values(1)",
                "REPLACE foo(id,msg) values(1, 'data')",
                "RePlACe InTo \"foo\" values (1, 'data')",
                "replace   into    `foo` /* hello*/ (one,too) values(1,2)"};
        String[] cmds2 = {"replace into bar.foo values(1)",
                "REPLACE bar.foo(id,msg) values(1, 'data')",
                "RePlAcE InTo \"bar\".\"foo\" values (1, 'data')",
                "replace   into    `bar`.`foo` /* hello*/ (one,too) values(1,2)"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.REPLACE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.REPLACE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test update with and without db name.
     */
    @Test
    public void testUpdate() throws Exception
    {
        String[] cmds1 = {"update /* comment */ foo set id=1",
                "UPDATE foo set id=1,msg='data' where \"msg\" = 'value'",
                "UpDaTe LOW_PRIORITY \"foo\" set id=1 WHere msg= 'data'",
                "update  `foo` /* hello*/ set id=1"};
        String[] cmds2 = {"update bar.foo set id=1",
                "UPDATE bar.foo set id=1,msg='data' where \"msg\" = 'value'",
                "UpDaTe \"bar\".\"foo\" set id=1 WHere msg= 'data'",
                "update  LOW_PRIORITY IGNORE   `bar`.`foo` /* hello*/ set id=1"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.UPDATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.UPDATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test delete with and without db name.
     */
    @Test
    public void testDelete() throws Exception
    {
        String[] cmds1 = {"/* comment */ delete /* comment */ from foo where id=1",
                "DELETE foo WHERE \"msg\" = 'value'",
                "DELETE \"foo\" WHere msg= 'data'",
                "delete    `foo` /* hello*/ where id=1",
                "DElete LOW_PRIORITY QUICK IGNORE \"foo\""};
        String[] cmds2 = {"delete from bar.foo where id=1",
                "DELETE bar.foo WHERE \"msg\" = 'value'",
                "DELETE \"bar\".\"foo\" WHere msg= 'data'",
                "delete    `bar`.`foo` /* hello*/ where id=1",
                "DElete LOW_PRIORITY QUICK IGNORE bar.\"foo\""};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DELETE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DELETE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test truncate with and without db name.
     */
    @Test
    public void testTruncate() throws Exception
    {
        String[] cmds1 = {"truncate table foo", "TRUNCATE TABLE foo",
                "TRUNCATE    tABlE  \"foo\" "};
        String[] cmds2 = {"truncate table bar.foo", "TRUNCATE TABLE bar.foo",
                "TRUNCATE    tABlE  \"bar\".\"foo\" "};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.TRUNCATE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.TRUNCATE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test LOAD DATA with and without db name.
     */
    @Test
    public void testLoadData() throws Exception
    {
        String[] cmds1 = {
                "load data local infile '/tmp/ld.txt' into table foo FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE foo",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE \"foo\"",
                "loAd   datA    lOcal iNfilE '/tmp/ld.txt' into   table \"foo\" FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/SQL_LOAD-1701-10901-48716.data' IGNORE "
                        + "INTO TABLE \"foo\" FIELDS TERMINATED BY ',' "
                        + "OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\' LINES TERMINATED "
                        + "BY '\n' STARTING BY '' IGNORE 0 LINES (@var0, @var1) SET id = "
                        + "@var0, data_10 = TRIM(TRIM(char(160) FROM TRIM(@var1))), "
                        + "call_now = '', upload_error_flag = 0, duplicate_flag = null"};
        String[] cmds2 = {
                "load data local infile '/tmp/ld.txt' into table bar.foo FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE bar.foo",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE \"bar\".\"foo\"",
                "loAd   datA    lOcal iNfilE '/tmp/ld.txt' into   table bar.\"foo\" FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/SQL_LOAD-1701-10901-48716.data' IGNORE "
                        + "INTO TABLE bar.foo FIELDS TERMINATED BY ',' "
                        + "OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\' LINES TERMINATED "
                        + "BY '\n' STARTING BY '' IGNORE 0 LINES (@var0, @var1) SET id = "
                        + "@var0, data_10 = TRIM(TRIM(char(160) FROM TRIM(@var1))), "
                        + "call_now = '', upload_error_flag = 0, duplicate_flag = null"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.LOAD_DATA, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.LOAD_DATA, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test SET commands.
     */
    @Test
    public void testSet() throws Exception
    {
        String[] cmds1 = {"SET @var0 := NULL",
                "set session binlog_format = row"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.SESSION,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.SET,
                    sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test create procedure with and without db name.
     */
    @Test
    public void testCreateProcedure() throws Exception
    {
        String[] cmds1 = {
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `foo`() begin select 1; end",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE foo() begin select 1; end"};
        String[] cmds2 = {
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `bar`.`foo`() begin select 1; end",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE bar.foo() begin select 1; end"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test drop procedure with and without db name.
     */
    @Test
    public void testDropProcedure() throws Exception
    {
        String[] cmds1 = {"drop procedure foo", "DROP PROCEDURE foo",
                "DrOp PROCEDUre \"foo\""};
        String[] cmds2 = {"drop procedure bar.foo", "DROP PROCEDURE bar.foo",
                "DRop  PRocedurE \"bar\".\"foo\""};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar", sqlName
                    .getSchema());
        }
    }

    /**
     * Test suppression of leading comments including comments generated by
     * mysqldump.
     */
    @Test
    public void testCommentHandling() throws Exception
    {
        String[] cmds1 = {"/* comment */ create table foo",
                "/*!50000 CREATE TABLE IF NOT EXISTS foo */",
                " /* another command */ creAtE TEMPORary TabLE \"foo\"",
                "/** a difficult comment */ create   table   `foo` /* hello*/",
                " /* comment*/create table foo"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test suppression of very large and/or double comments.
     */
    @Test
    public void testCommentHandling2() throws Exception
    {
        String cmd1 = "/* " + 
        "comment comment comment comment comment comment comment comment comment comment comment comment" + 
        "comment comment comment comment comment comment comment comment comment comment comment comment" + 
        "comment comment comment comment comment comment comment comment comment comment comment comment" + 
        "comment comment comment comment comment comment comment comment comment comment comment comment" + 
        "comment comment comment comment comment comment comment comment comment comment comment comment" + 
        "comment comment comment comment comment comment comment comment comment comment comment comment" + 
        "comment comment comment comment comment comment comment comment comment comment comment comment" + 
        "comment comment comment comment comment comment comment comment comment comment comment comment" + 
        "comment comment comment comment comment comment comment comment comment comment comment comment" + 
        " */ create table foo";
        String cmd2 = "/* comment */ create table foo /* comment */";
        String[] cmds1 = {cmd1, cmd2};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test begin/start transaction.
     */
    @Test
    public void testBegin() throws Exception
    {
        String[] cmds1 = {" start transaction WITH CONSISTENT SNAPSHOT",
                "StarT TransactioN", "begin", " BEGIN WORK "};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd,
                    SqlOperation.TRANSACTION, sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.BEGIN,
                    sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test commit
     */
    @Test
    public void testCommit() throws Exception
    {
        String[] cmds1 = {"COMMIT", "commit work", " cOMmit WorK", "commit"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd,
                    SqlOperation.TRANSACTION, sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.COMMIT,
                    sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * This is experimental--case copied from MySQLExtractor. Not sure it's even
     * legal in SQL.
     */
    @Test
    public void testBeginEnd() throws Exception
    {
        String[] cmds1 = {"begin select 1; end"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.BLOCK,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.BEGIN_END, sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Identify a select. We don't select the db.table as select syntax is quite
     * convoluted.
     */
    @Test
    public void testSelect() throws Exception
    {
        String[] cmds1 = {
                "SELECT t1.*, t2.* FROM t1 INNER JOIN t2",
                "SELECT CONCAT(last_name,', ',first_name) AS full_name FROM mytable ORDER BY full_name"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.SELECT, sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test performance over a large number of inserts.
     */
//    @Test
    public void testInsertMany() throws Exception
    {
        String[] cmds1 = {
                "insert into foo values(1)",
                "INSERT INTO foo(id,msg) values(1, 'data')",
                "InSeRt InTo \"foo\" values (1, 'data')",
                "LOAD DATA INFILE '/tmp/SQL_LOAD-1701-10901-48716.data' IGNORE "
                        + "INTO TABLE \"foo\" FIELDS TERMINATED BY ',' "
                        + "OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\' LINES TERMINATED "
                        + "BY '\n' STARTING BY '' IGNORE 0 LINES (@var0, @var1) SET id = "
                        + "@var0, data_10 = TRIM(TRIM(char(160) FROM TRIM(@var1))), "
                        + "call_now = '', upload_error_flag = 0, duplicate_flag = null"};
        SqlOperationMatcher m = new MySQLOperationMatcher();

        for (int i = 0; i < 1000000; i++)
        {
            String cmd = cmds1[i % cmds1.length];
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());

            if (i % 100000 == 0)
            {
                logger.info("Statements parsed: " + i);
            }
        }
    }
}