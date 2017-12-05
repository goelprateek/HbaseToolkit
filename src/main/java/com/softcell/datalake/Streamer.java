package com.softcell.datalake;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.client.model.Filters.eq;

public class Streamer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Streamer.class);

    private static final byte[] STATE_TABLE_COL_FAMILY = "t".getBytes();

    private static final byte[] STATE_TABLE_COL_QUALIFIER_OPTIME = "optime".getBytes();

    private static final byte[] STATE_TABLE_COL_QUALIFIER_INC = "inc".getBytes();

    private final Configuration _conf;

    private final MongoClient _mongo;

    private final Connection _hBaseConnection;

    private final HashMap<String, Table> _knownTables;

    private final Table _stateTable;

    private final byte[] _tailerID;

    private final Translator _translator;

    private final boolean _skipUpdates;

    private final boolean _skipDeletes;

    private final boolean _bufferWrites;

    private final AtomicBoolean _running = new AtomicBoolean(false);

    private int _optime = 0;

    private int _inc = 0;

    private boolean _optimeSet = false;

    public Streamer(Configuration conf, MongoClient mongo, Connection hBaseConnection, String tailerName) {
        _conf = conf;
        _mongo = mongo;
        _knownTables = new HashMap<>();
        _translator = ConfigUtil.getTranslator(_conf);
        this._hBaseConnection = hBaseConnection;
        _stateTable = createStateTable();

        if (tailerName == null) {
            List<ServerAddress> addresses = _mongo.getAllAddress();
            tailerName = StringUtils.join(addresses, ",");
        }

        _tailerID = tailerName.getBytes();

        _skipUpdates = ConfigUtil.getSkipUpdates(_conf);
        _skipDeletes = ConfigUtil.getSkipDeletes(_conf);
        _bufferWrites = ConfigUtil.getBufferWrites(_conf);
    }

    public Streamer(Configuration conf, MongoClient mongo, Connection hBaseConnection) {
        this(conf, mongo, hBaseConnection, null);
    }

    public static void main(String[] args) throws Exception {
        CommandLineOptions opts = new CommandLineOptions();
        JCommander parser = new JCommander(opts, args);

        if (opts.help) {
            parser.usage();
            return;
        }

        Configuration conf = HBaseConfiguration.create();

        if (opts.translatorClass != null) {
            LOGGER.info("Setting the translator class to " + opts.translatorClass);

            try {
                @SuppressWarnings("unchecked")
                Class<? extends Translator> translatorClass = ClassUtils.getClass(opts.translatorClass);
                ConfigUtil.setTranslatorClass(conf, translatorClass);
            } catch (ClassNotFoundException e) {
                LOGGER.error("Couldn't find translator class: " + opts.translatorClass, e);
                return;
            }
        }

        if (opts.skipUpdates) {
            ConfigUtil.setSkipUpdates(conf, true);
        }

        if (opts.skipDeletes) {
            ConfigUtil.setSkipDeletes(conf, true);
        }

        if (opts.bufferWrites) {
            if (!opts.skipUpdates || !opts.skipDeletes) {
                LOGGER.warn("--buffer-writes is set, but you're missing --skip-updates and/or --skip-deletes! You should be careful, because ordering may not work properly.");
            }

            ConfigUtil.setBufferWrites(conf, true);
        }

        MongoClientURI uri = new MongoClientURI(opts.mongoURI);

        MongoClient mongo = new MongoClient(uri);

        Connection connection = ConnectionFactory.createConnection(conf);

        Streamer streamer = new Streamer(conf, mongo, connection, opts.tailerName);
        Thread currentThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new TailerCleanupThread(streamer, currentThread));
        streamer.stream();

        return;
    }

    private Table createStateTable() {

        String stateTableName = ConfigUtil.getTailerStateTable(_conf);

        Admin admin = getHBaseAdmin();

        TableName tableName = TableName.valueOf(stateTableName);

        try {

            if (!admin.tableExists(tableName)) {

                HColumnDescriptor familyDesc = new HColumnDescriptor(STATE_TABLE_COL_FAMILY);
                familyDesc.setMaxVersions(1);
                HTableDescriptor tableDesc = new HTableDescriptor(tableName)
                        .addFamily(familyDesc);

                admin.createTable(tableDesc);
            }


            return getHBaseTable(stateTableName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create state table", e);
        }
    }

    private Admin getHBaseAdmin() {
        try {

            return _hBaseConnection.getAdmin();

        } catch (Exception e) {
            throw new RuntimeException("Failed to (re-)connect to HBase", e);
        }
    }

    private Table getHBaseTable(String tableName) {
        try {

            return _hBaseConnection.getTable(TableName.valueOf(tableName));


        } catch (Exception e) {
            throw new RuntimeException("Failed to create table in  HBase", e);
        }


    }

    public void stream() {
        if (!_running.compareAndSet(false, true)) return;

        MongoCursor<Document> cursor = createCursor().iterator();

        try {

            while (_running.get() && cursor.hasNext()) {
                Document next = cursor.next();
                handleOp(new BasicDBObject(next));
            }

        } finally {
            saveOptime();
            cursor.close();
            closeTables();
        }
    }

    public void stop() {
        LOGGER.info(" shutting down ...");
        _running.set(false);
    }

    private FindIterable<Document> createCursor() {

        MongoCollection<Document> oplog = _mongo.getDatabase("local").getCollection("oplog.rs");

        BSONTimestamp startingTimestamp = getStartingTimestamp();

        FindIterable<Document> documents;


        if (startingTimestamp == null) {

            LOGGER.info(" Tailing the oplog from the beginning...");

            documents = oplog.find();

        } else {

            LOGGER.info(" Tailing the oplog from " + startingTimestamp);

            BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", startingTimestamp));

            documents = oplog.find(query).oplogReplay(true);

        }

        documents.noCursorTimeout(true).cursorType(CursorType.Tailable).maxAwaitTime(100, TimeUnit.SECONDS);

        return documents;

    }

    private BSONTimestamp getStartingTimestamp() {
        Get get = new Get(_tailerID);

        Result res;
        try {
            res = _stateTable.get(get);
        } catch (IOException e) {
            LOGGER.error("Failed to get a starting timestamp for tailer ID: " + _tailerID);
            return null;
        }

        if (res.isEmpty()) {
            if (ConfigUtil.getSkipBacklog(_conf))
                return new BSONTimestamp((int) (System.currentTimeMillis() / 1000L), 0);

            return null;
        } else {

            byte[] raw_optime = res.getValue(STATE_TABLE_COL_FAMILY, STATE_TABLE_COL_QUALIFIER_OPTIME);

            byte[] raw_inc = res.getValue(STATE_TABLE_COL_FAMILY, STATE_TABLE_COL_QUALIFIER_INC);

            _optime = Integer.parseInt(new String(raw_optime));
            _inc = Integer.parseInt(new String(raw_inc));
            _optimeSet = true;
            return new BSONTimestamp(_optime, _inc);
        }
    }

    protected void handleOp(BasicDBObject doc) {
        String type = (String) doc.getString("op"),
                ns = (String) doc.getString("ns");

        updateOptime(doc);

        if (type.equals("n") || type.equals("c")) return;

        String[] parts = ns.split("\\.", 2);
        String database = parts[0], collection = parts[1];
        if (collection.startsWith("system.")) return;

        String tableName = _translator.mapNamespaceToHBaseTable(database, collection);
        Table table;

        // skip tables that we're skipping
        if (tableName == null) return;

        try {
            table = ensureTable(tableName);
        } catch (Exception e) {
            LOGGER.error("Failed to create table " + tableName + " for op: " + doc, e);
            return;
        }

        Document data = (Document) doc.get("o");

        if (type.equals("i")) {
            handleInsert(table, data);
        } else if (!_skipUpdates && type.equals("u")) {
            Document selector = (Document) doc.get("o2");
            handleUpdate(table, data, selector, database, collection);
        } else if (!_skipDeletes && type.equals("d")) {
            handleDelete(table, data);
        }
    }

    protected void handleInsert(Table table, Document doc) {
        byte[] row = _translator.createRowKey(doc);
        Put put = _translator.createPut(row, doc);

        try {
            table.put(put);
        } catch (IOException e) {
            LOGGER.error("Failed trying to insert object at " + row + " in " + table, e);
        }
    }

    protected void handleUpdate(Table table, Document doc, Document selector,
                                String database, String collection) {
        // for $set, etc, grab the whole document from mongo
        boolean resync = false;
        for (String key : doc.keySet()) {
            if (key.startsWith("$")) {
                resync = true;
                break;
            }
        }

        Object id = selector.get("_id");

        if (resync) {

            MongoCollection<Document> collection1 = _mongo.getDatabase(database).getCollection(collection)
                    .withReadPreference(ReadPreference.primaryPreferred());
            Document id1 = collection1.find(eq("_id", id)).first();
            doc = new Document(id1);

            // the document may have since been removed
            if (doc == null) return;
        } else {
            // the document itself is usually missing _id (but it's always present in the selector)
            doc.put("_id", id);
        }

        byte[] row = _translator.createRowKey(selector);
        Put put = _translator.createPut(row, doc);

        try {
            table.put(put);
        } catch (IOException e) {
            LOGGER.error("Failed trying to update object at " + row + " in " + table, e);
        }
    }

    protected void handleDelete(Table table, Document selector) {
        byte[] row = _translator.createRowKey(selector);
        Delete del = new Delete(row);

        try {
            table.delete(del);
        } catch (IOException e) {
            LOGGER.error("Failed trying to delete object at " + row + " in " + table, e);
        }
    }

    private Table ensureTable(String tableName) throws Exception {
        if (_knownTables.containsKey(tableName)) {
            return _knownTables.get(tableName);
        }

        Admin admin = getHBaseAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            HTableDescriptor tableDesc = _translator.describeHBaseTable(tableName);
            admin.createTable(tableDesc);
        }

        Table table = getHBaseTable(tableName);

        if (_bufferWrites) {
            admin.flush(TableName.valueOf(tableName)); // Force a flush of the created data.
        }

        _knownTables.put(tableName, table);
        return table;
    }

    private void closeTables() {
        for (Table table : _knownTables.values()) {
            try {
                table.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close HBase table: " + table);
            }
        }
    }

    private void updateOptime(BasicDBObject doc) {
        BsonTimestamp ts = (BsonTimestamp) doc.get("ts");
        int optime = ts.getTime(), inc = ts.getInc();

        // only checkpoint every 60 seconds
        if (!_optimeSet || (optime - _optime) >= 60) {
            _optime = optime;
            _inc = inc;
            _optimeSet = true;

            LOGGER.info("optime: " + _optime);
            saveOptime();
        }
    }

    private void saveOptime() {
        if (!_optimeSet) return;

        Put put = new Put(_tailerID);
        put.addColumn(STATE_TABLE_COL_FAMILY, STATE_TABLE_COL_QUALIFIER_OPTIME, Integer.toString(_optime).getBytes());
        put.addColumn(STATE_TABLE_COL_FAMILY, STATE_TABLE_COL_QUALIFIER_INC, Integer.toString(_inc).getBytes());

        try {
            _stateTable.put(put);
        } catch (IOException e) {
            LOGGER.error("Failed writing optime to state table!", e);
        }
    }

    private static class CommandLineOptions {
        @Parameter(names = {"-m", "--mongo"}, description = "The base MongoDB URI to connect to", required = true)
        private String mongoURI;

        @Parameter(names = {"-t", "--translator"}, description = "Specify a ZWTranslator class to use. Defaults to com.stripe.zerowing.ZWBasicTranslator")
        private String translatorClass;

        @Parameter(names = {"-n", "--tailer-name"}, description = "The name to save the tailer's optime under. By default, this is the combined hostnames of all replica set members.")
        private String tailerName;

        @Parameter(names = "--skip-updates", description = "Skip update operations - when a record is updated in MongoDB, don't update it in HBase")
        private boolean skipUpdates;

        @Parameter(names = "--skip-deletes", description = "Skip delete operations - when a record is deleted in MongoDB, don't delete it in HBase")
        private boolean skipDeletes;

        @Parameter(names = "--buffer-writes", description = "Buffer writes using the HBase client API. You should only use this with insert-only tailing, because it destroys ordering in most cases (and is generally unsafe, besides).")
        private boolean bufferWrites;

        @Parameter(names = "--help", help = true, description = "Show this message")
        private boolean help;
    }

    private static class TailerCleanupThread extends Thread {

        private final Streamer _streamer;
        private final Thread _tailerThread;


        public TailerCleanupThread(Streamer _streamer, Thread _tailerThread) {
            this._streamer = _streamer;
            this._tailerThread = _tailerThread;
        }

        @Override
        public void run() {

            try {

                _streamer.stop();
                _tailerThread.join();

            } catch (InterruptedException e) {
                LOGGER.info(" having hard time in going down ! ");
            }
        }
    }

}
