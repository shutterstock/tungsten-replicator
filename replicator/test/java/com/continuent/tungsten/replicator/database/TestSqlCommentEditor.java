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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests SQL comment editin.
 * <p/>
 * As currently written, this test includes checks on MySQL schema. It needs to
 * be expanded to work with other database types, such as Drizzle.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestSqlCommentEditor
{
    SqlOperationMatcher matcher = new MySQLOperationMatcher();
    MySQLCommentEditor  editor  = new MySQLCommentEditor();

    /**
     * Test adding and recognizing comments on basic DDL statements.
     */
    @Test
    public void testBasicDdl() throws Exception
    {
        String[] cmds = {"create database foo",
                "create table if not exists `test`.`foo`",
                "drop procedure test.foo"};
        String comment = "___SERVICE = [mysvc]___";
        String commentRegex = "___SERVICE = \\[([a-zA-Z0-9-_]+)\\]___";
        editor.setCommentRegex(commentRegex);

        for (String cmd : cmds)
        {
            SqlOperation op = matcher.match(cmd);
            String newCmd = editor.addComment(cmd, op, comment);
            String foundComment = editor.fetchComment(newCmd, op);
            Assert.assertTrue("Comment added", newCmd.length() > cmd.length());
            Assert
                    .assertEquals("Found original comment", comment,
                            foundComment);
        }
    }

    /**
     * Confirm no comment is returned if none exists.
     */
    @Test
    public void testNonExistentComment() throws Exception
    {
        String[] cmds = {"create database foo",
                "create table if not exists `test`.`foo`",
                "drop procedure test.foo /* +++SERVICE = [mysvc]+++ */"};
        String commentRegex = "___SERVICE = \\[([a-zA-Z0-9-_]+)\\]___";
        editor.setCommentRegex(commentRegex);

        for (String cmd : cmds)
        {
            SqlOperation op = matcher.match(cmd);
            String foundComment = editor.fetchComment(cmd, op);
            Assert.assertNull("No comment found", foundComment);
        }
    }

    /**
     * Test adding and recognizing comments on stored procedure definitions.
     */
    @Test
    public void testSproc() throws Exception
    {
        String[] cmds = {
                "CREATE PROCEDURE simpleproc2 (OUT param1 INT) \nBEGIN\n"
                        + "SELECT 1 INTO param1;\nEND",
                "create procedure `test`.`simpleproc2` (OUT param1 INT) \n"
                        + "BEGIN\nSELECT 1 INTO param1;\nEND",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `simpleproc2`(OUT param1 INT)\n"
                        + "    COMMENT 'this is a comment'\n"
                        + "begin SELECT 1 INTO param1;end",
                "CREATE DEFINER=`root`@`localhost` procedure `simpleproc2`(OUT param1 INT)\n"
                        + "    comment 'this is a comment'\n"
                        + "begin SELECT 1 INTO param1;end"};
        String comment = "___SERVICE = [mysvc]___";
        String commentRegex = "___SERVICE = \\[([a-zA-Z0-9-_]+)\\]___";
        editor.setCommentRegex(commentRegex);

        for (String cmd : cmds)
        {
            SqlOperation op = matcher.match(cmd);
            String newCmd = editor.addComment(cmd, op, comment);
            String foundComment = editor.fetchComment(newCmd, op);
            Assert.assertTrue("Comment must be added", newCmd.length() > cmd
                    .length());
            Assert
                    .assertEquals("Found original comment", comment,
                            foundComment);
        }
    }

    /**
     * Verify that we return appendable comments when safe to do so, otherwise
     * null.
     */
    @Test
    public void testAppendableComment() throws Exception
    {
        String[] cmds1 = {"create database foo",
                "create table if not exists `test`.`foo`",
                "drop procedure test.foo /* +++SERVICE = [mysvc]+++ */"};
        String[] cmds2 = {
                "CREATE PROCEDURE simpleproc2 (OUT param1 INT) \nBEGIN\n"
                        + "SELECT 1 INTO param1;\nEND",
                "create procedure `test`.`simpleproc2` (OUT param1 INT) \n"
                        + "BEGIN\nSELECT 1 INTO param1;\nEND",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `simpleproc2`(OUT param1 INT)\n"
                        + "    COMMENT 'this is a comment'\n"
                        + "begin SELECT 1 INTO param1;end",
                "CREATE DEFINER=`root`@`localhost` procedure `simpleproc2`(OUT param1 INT)\n"
                        + "    comment 'this is a comment'\n"
                        + "begin SELECT 1 INTO param1;end"};

        String comment = "___SERVICE = [mysvc]___";
        String commentRegex = "___SERVICE = \\[([a-zA-Z0-9-_]+)\\]___";
        editor.setCommentRegex(commentRegex);

        // Test cases that generate comments. 
        for (String cmd : cmds1)
        {
            SqlOperation op = matcher.match(cmd);
            String formattedComment = editor.formatAppendableComment(op, comment);
            Assert.assertNotNull("Must have comment", formattedComment);
        }

        // Test cases that do not. 
        for (String cmd : cmds2)
        {
            SqlOperation op = matcher.match(cmd);
            String formattedComment = editor.formatAppendableComment(op, comment);
            Assert.assertNull("Must not have comment", formattedComment);
        }
    }
}