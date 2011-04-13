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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.extractor.ExtractorWrapper;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.SingleThreadStageTask;

/**
 * This class defines a BinlogTest.  It requires a MySQL server in order to 
 * run. 
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class BinlogTest extends TestCase
{
    // static Logger logger = Logger.getLogger(BinlogTest.class);
    static Logger logger = null;

    protected void setUp() throws Exception
    {
        super.setUp();
        if (logger == null)
        {
            BasicConfigurator.configure();
            logger = Logger.getLogger(BinlogTest.class);
            logger.info("logger initialized");
        }
    }

    public void test5Binlog() throws Exception
    {
        try
        {
            // Set properties.
            TungstenProperties conf = this.createConfProperties();
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql.binlog_dir",
                    ".");
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT
                    + ".mysql.binlog_file_pattern", "binlog_5events");

            // Configure runtime with these properties and prepare the
            // extractor for use.
            ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                    new MockOpenReplicatorContext(), ReplicatorMonitor
                            .getInstance());
            runtime.configure();
            MySQLExtractor extractor = getMySQLExtractor(runtime);
            extractor.setStrictVersionChecking(false);
            extractor.prepare(runtime);
            extractor.setLastEventId("000001:0");

            /* read all 5 events from file */
            logger.info("extractor starting");
            for (int i = 0; i < 4; i++)
            {
                extractor.extract();
            }
            logger.info("extractor finished");
        }
        catch (MySQLExtractException e)
        {
            fail(e.getMessage());
            logger.info("extractor failed");
        }
        return;
    }

    public void test_trx() throws Exception
    {
        try
        {
            // Set properties.
            TungstenProperties conf = this.createConfProperties();
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql.binlog_dir",
                    ".");
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT
                    + ".mysql.binlog_file_pattern", "binlog_trx");

            // Configure runtime with these properties and prepare the
            // extractor for use.
            ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                    new MockOpenReplicatorContext(), ReplicatorMonitor
                            .getInstance());
            runtime.configure();
            MySQLExtractor extractor = getMySQLExtractor(runtime);
            extractor.setStrictVersionChecking(false);
            extractor.prepare(runtime);
            extractor.setLastEventId("000001:0");

            /* read all transactions from file */
            logger.info("extractor starting");
            for (int i = 0; i < 5; i++)
            {
                extractor.extract();
            }
            logger.info("extractor finished");
        }
        catch (MySQLExtractException e)
        {
            fail(e.getMessage());
            logger.info("extractor failed");
        }
        return;
    }

    public void testBinlogRBR() throws Exception
    {
        try
        {
            // Set properties.
            TungstenProperties conf = this.createConfProperties();
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql.binlog_dir",
                    ".");
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT
                    + ".mysql.binlog_file_pattern", "binlog_rbr_1");

            // Configure runtime with these properties and prepare the
            // extractor for use.
            ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                    new MockOpenReplicatorContext(), ReplicatorMonitor
                            .getInstance());
            runtime.configure();
            MySQLExtractor extractor = getMySQLExtractor(runtime);
            extractor.setStrictVersionChecking(false);
            extractor.prepare(runtime);
            extractor.setLastEventId("000001:0");

            /* read all 5 events from file */
            logger.info("extractor starting");
            for (int i = 0; i < 4; i++)
            {
                extractor.extract();
            }
            logger.info("extractor finished");
        }
        catch (MySQLExtractException e)
        {
            fail(e.getMessage());
            logger.info("extractor failed");
        }
        return;
    }

    public void testApplierRBR() throws Exception
    {
        if (true)
            return;
        
        // Code clean-up : commenting out dead code
//        try
//        {
//            // Set properties.
//            TungstenProperties conf = this.createConfProperties();
//            conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql.binlog_dir",
//                    ".");
//            conf.setString(ReplicatorConf.EXTRACTOR_ROOT
//                    + ".mysql.binlog_file_pattern", "mysql-bin-row");
//
//            // Configure runtime with these properties and prepare the
//            // extractor for use.
//            ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
//                    new MockOpenReplicatorContext(), ReplicatorMonitor
//                            .getInstance());
//            runtime.configure();
//
//            MySQLExtractor extractor = getMySQLExtractor(runtime);
//            extractor.setStrictVersionChecking(false);
//            extractor.prepare(runtime);
//            extractor.setLastEventId("000003:0");
//
//            // Extract events. Make sure we get expected number (7).
//            runtime.prepare();
//            Pipeline pipeline = runtime.getPipeline();
//            pipeline.start(new EventDispatcher());
//            Future<ReplDBMSEvent> future = pipeline
//                    .watchForAppliedSequenceNumber(6);
//            future.get(3, TimeUnit.SECONDS);
//            /**
//             * Applier applier = runtime.getApplier(); applier.prepare(runtime);
//             * // applier.configure(); // read all 5 events from file
//             * logger.info("RBR extractor/applier starting"); for (int i = 0; i
//             * < 7; i++) { DBMSEvent event = extractor.extract(); if (event !=
//             * null) { applier.apply(event, i, true); } }
//             */
//            logger.info("RBR extractor/applier finished");
//        }
//        catch (MySQLExtractException e)
//        {
//            fail(e.getMessage());
//            logger.info("RBR extractor/applier failed");
//        }
//        return;
    }

    // Generate a simple runtime.
    private TungstenProperties createConfProperties()
            throws ReplicatorException
    {
        TungstenProperties conf = new TungstenProperties();
        conf.setString(ReplicatorConf.SERVICE_NAME, "test");
        conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_MASTER);
        conf.setString(ReplicatorConf.PIPELINES, "master");
        conf.setString(ReplicatorConf.PIPELINE_ROOT + ".master", "extract");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract",
                SingleThreadStageTask.class.toString());
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.extractor",
                "mysql");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.applier", "dummy");

        conf.setString(ReplicatorConf.APPLIER_ROOT + ".dummy",
                DummyApplier.class.getName());
        conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql",
                MySQLExtractor.class.getName());

        return conf;
    }

    // Fetch the MySQL extractor from current pipeline.
    private MySQLExtractor getMySQLExtractor(ReplicatorRuntime runtime)
    {
        Pipeline p = runtime.getPipeline();
        ExtractorWrapper wrapper = (ExtractorWrapper) p.getStages().get(0)
                .getExtractor0();
        return (MySQLExtractor) wrapper.getExtractor();
    }
}
