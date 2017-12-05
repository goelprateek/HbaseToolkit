package com.softcell.datalake;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.util.*;

public class BulkImportRunner implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportRunner.class);

    private LinkedList<BulkImportJob> _jobs = new LinkedList<>();


    private Configuration _conf;

    private Admin _hbaseAdmin;

    private Translator _translator;


    public BulkImportRunner() {
        this(HBaseConfiguration.create());
    }

    public BulkImportRunner(Configuration conf) {
        _conf = conf;
    }

    @Override
    public Configuration getConf() {
        return _conf;
    }

    @Override
    public void setConf(Configuration configuration) {
        _conf = configuration;
    }

    public Admin getHBaseAdmin() {
        if (_hbaseAdmin != null) return _hbaseAdmin;

        try {

            Connection connection = ConnectionFactory.createConnection(_conf);

            _hbaseAdmin = connection.getAdmin();

        } catch (Exception e) {
            throw new RuntimeException(" Error connecting to HBase with probable cause {} ", e);
        }

        return _hbaseAdmin;
    }


    public Translator getTranslator() {
        if (_translator != null) return _translator;

        _translator = ConfigUtil.getTranslator(_conf);
        return _translator;
    }

    public void addJob(String mongoURI) {

        MongoClientURI uri = new MongoClientURI(mongoURI);

        String database = uri.getDatabase(), collection = uri.getCollection();
        String tableName = getTranslator().mapNamespaceToHBaseTable(database, collection);

        if (tableName == null) {
            LOGGER.info("Skipping namespace '" + database + "." + collection + "' because it doesn't map to an HBase table");
            return;
        }

        addJob(uri, tableName);
    }

    public void addJobsForNamespace(String rawURI, String database, String collection) {

        MongoClientURI uri = new MongoClientURI(rawURI);

        MongoClient mongo = new MongoClient(uri);

        if (database != null && collection != null) {

            if(null == mongo.getDatabase(database).getCollection(collection)){
                throw new IllegalArgumentException(" Couldn't find namespace: " + database + "." + collection);
            }


            buildJob(uri, database, collection);
            return;
        } else if (database == null && collection != null) {
            throw new IllegalArgumentException("You can't specify a MongoDB collection without also specifying a database");
        }

        List<String> databases = new ArrayList<>(1);
        if (database != null) {

            databases.add(database);

        } else {

            MongoCursor<String> iterator = mongo.listDatabaseNames().iterator();

            while (iterator.hasNext()) {
                databases.add(iterator.next());
            }

        }

        for (String db : databases) {
            if (db.equals("local") || db.equals("admin")) continue;

            Set<String> collections = Sets.newHashSet(mongo.getDatabase(db).listCollectionNames());
            for (String coll : collections) {
                if (coll.startsWith("system.")) continue;
                buildJob(uri, db, coll);
            }
        }
    }

    public boolean doBulkImport() throws Exception {
        if (!createTables()) return false;

        for (BulkImportJob job : _jobs) {
            try {
                job.submit();
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("Error while running job", e);
                return false;
            }
        }

        boolean success = true;
        for (BulkImportJob job : _jobs) {
            job.getJob().waitForCompletion(true);

            if (job.getJob().isSuccessful()) {
                job.completeImport();
            } else {
                success = false;
            }
        }

        return true;
    }

    private boolean createTables() throws Exception {
        Translator translator = getTranslator();
        Admin admin = getHBaseAdmin();
        HashSet<String> createdTables = new HashSet<String>();

        for (BulkImportJob job : _jobs) {
            String tableName = job.getOutputTableName();

            if (!createdTables.contains(tableName)) {
                if (admin.tableExists(TableName.valueOf(tableName))) {
                    if (ConfigUtil.getMergeExistingTable(_conf)) {
                        continue;
                    } else {
                        LOGGER.error("Table already exists: " + tableName + ". If you want to merge with an existing table, use the --merge option");
                        return false;
                    }
                }

                HTableDescriptor tableDesc = translator.describeHBaseTable(tableName);

                try {
                    if (ConfigUtil.getPresplitTable(_conf)) {
                        byte[][] splits = calculateRegionSplits(job.getMongoURI(), tableName);
                        admin.createTable(tableDesc, splits);
                    } else {
                        admin.createTable(tableDesc);
                    }
                } catch (IOException e) {
                    LOGGER.error("Failed to create table: " + tableName, e);
                    return false;
                }

                createdTables.add(tableName);
            }
        }

        return true;
    }

    private byte[][] calculateRegionSplits(MongoClientURI uri, String tableName) throws Exception {

        MongoClient mongoClient = new MongoClient(uri);

        long size = mongoClient.getDatabase(uri.getDatabase()).getCollection(uri.getCollection()).count();

        long regionSize = ConfigUtil.getPresplitTableRegionSize(_conf);

        int numRegions = (int) Math.min((size / regionSize) + 1, 4096);

        if (numRegions > 1) {

            LOGGER.info("Pre-splitting " + tableName + " into " + numRegions + " regions");

            RegionSplitter.UniformSplit splitter = new RegionSplitter.UniformSplit();

            return splitter.split(numRegions);

        } else {

            LOGGER.info("Not splitting " + tableName + ", because the data can fit into a single region");

            return new byte[0][0];
        }
    }

    private void buildJob(MongoClientURI uri, String database, String collection) {
        UriBuilder builder = UriBuilder.fromUri(uri.toString());
        String builtURI = builder.path("{db}.{coll}").build(database, collection).toString();

        addJob(builtURI);
    }

    private void addJob(MongoClientURI uri, String tableName) {
        LOGGER.info("Adding a job for " + uri + " -> " + tableName);

        try {
            BulkImportJob job = new BulkImportJob(_conf, uri, tableName);
            job.getJob().setJarByClass(BulkImportRunner.class); //TODO
            _jobs.add(job);
        } catch (IOException e) {
            LOGGER.error("Failed to add job", e);
        }
    }

    private static class CommandLineOptions {
        @Parameter(names = {"-m", "--mongo"}, description = "The base MongoDB URI to connect to", required = true)
        private String mongoURI;

        @Parameter(names = {"-d", "--database"}, description = "The MongoDB database to import")
        private String mongoDatabase;

        @Parameter(names = {"-c", "--collection"}, description = "The MongoDB collection to import")
        private String mongoCollection;

        @Parameter(names = {"-t", "--translator"}, description = "Specify a ZWTranslator class to use. Defaults to com.stripe.zerowing.ZWBasicTranslator")
        private String translatorClass;

        @Parameter(names = "--merge", description = "Copy into existing tables. Unless this option is set, the import will fail if any tables already exists")
        private boolean merge;

        @Parameter(names = "--tmp-dir", description = "The temporary HDFS directory to write HFiles to (will be created). Defaults to /tmp/zerowing_hfile_out")
        private String tmpDir;

        @Parameter(names = "--no-split", description = "Don't presplit HBase tables")
        private boolean noSplit;

        @Parameter(names = "--region-size", description = "Size of regions to presplit HBase tables into. Defaults to 10gb")
        private String regionSize;

        @Parameter(names = "--mongo-split-size", description = "Size of chunks to split Mongo collections into. Defaults to 8mb")
        private String mongoSplitSize;

        @Parameter(names = "--help", help = true, description = "Show this message")
        private boolean help;
    }


    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(args).getRemainingArgs();
        CommandLineOptions opts = new CommandLineOptions();
        JCommander parser = new JCommander(opts, remainingArgs);

        if (opts.help) {
            parser.usage();
            return 0;
        }

        if (opts.translatorClass != null) {
            LOGGER.info("Setting the translator class to " + opts.translatorClass);

            try {
                @SuppressWarnings("unchecked")
                Class<? extends Translator> translatorClass = ClassUtils.getClass(opts.translatorClass);
                ConfigUtil.setTranslatorClass(_conf, translatorClass);
            } catch (ClassNotFoundException e) {
                LOGGER.error("Couldn't find translator class: " + opts.translatorClass, e);
                return 1;
            }
        }

        if (opts.tmpDir != null) {
            LOGGER.info("Using " + opts.tmpDir + " as the temporary HFile path");
            ConfigUtil.setTemporaryHFilePath(_conf, opts.tmpDir);
        }

        if (opts.noSplit) {
            ConfigUtil.setPresplitTable(_conf, false);
        }

        if (opts.merge) {
            ConfigUtil.setMergeExistingTable(_conf, true);
        }

        if (opts.regionSize != null) {
            ConfigUtil.setPresplitTableRegionSize(_conf, Integer.parseInt(opts.regionSize));
        }

        if (opts.mongoSplitSize != null) {
            MongoConfigUtil.setSplitSize(_conf, Integer.parseInt(opts.mongoSplitSize));
        }

        MongoClientURI uri = new MongoClientURI(opts.mongoURI);

        String database = null;
        if (opts.mongoDatabase != null) {
            database = opts.mongoDatabase;
        } else if (uri.getDatabase() != null) {
            database = uri.getDatabase();
        }

        String collection = null;
        if (opts.mongoCollection != null) {
            collection = opts.mongoCollection;
        } else if (uri.getCollection() != null) {
            collection = uri.getCollection();
        }

        addJobsForNamespace(opts.mongoURI, database, collection);

        if (!doBulkImport()) {
            return 1;
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new BulkImportRunner(), args));
    }

}
