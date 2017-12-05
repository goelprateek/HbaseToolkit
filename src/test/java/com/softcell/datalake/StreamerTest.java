package com.softcell.datalake;


import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.bson.BSONDecoder;
import org.bson.BasicBSONDecoder;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class StreamerTest {

    private static MongoClient mongoClient;


    private static Connection connection;

    private String name;

    private TableName tableName;

    private TailerThread tailerThread;

    @BeforeClass
    public static void setUpClass() throws Exception {
        try {

            // try the normal way first, to avoid chunder
            MongoClient testMongoClient = new MongoClient("localhost", 37017);
            testMongoClient.listDatabaseNames();

            // this is necessary to connnect as a replica set
            mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:37017,localhost:37018/?slaveOk=true"));
            mongoClient.listDatabaseNames();
            assumeNotNull(mongoClient.getReplicaSetStatus());
        } catch (Exception e) {
            System.out.println("error connecting to mongo: " + e);
            assumeTrue(false);
        }

    }

    @Before
    public void setUp() throws Exception {
        try {
            mongoClient.getDatabase("_test_hbase").drop();
            connection = ConnectionFactory.createConnection();
        } catch (Exception e) {
            System.out.println("error setting up mongo: " + e);
            assumeTrue(false);
        }

        name = UUID.randomUUID().toString();
        tableName = TableName.valueOf("zw._test_hbase." + name);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        connection.close();
    }

    @Test
    public void testBasicTailer() throws Exception {

        Configuration conf = connection.getConfiguration();

        ConfigUtil.setDefaultFsDirectoryKey(conf, "/tmp/hbase-staging");

        ConfigUtil.setSkipBacklog(conf, true);

        Admin admin = connection.getAdmin();

        assertFalse(admin.tableExists(tableName));

        startTailer(conf);
        createOps();
        stopTailer();

        assertTrue(admin.tableExists(tableName));

        Table table = connection.getTable(tableName);

        assertEquals(2, getCount(table));

        int[] values = getValues(table, "a");
        assertEquals(2, values.length);
        assertEquals(4, values[0]);
        assertEquals(1, values[1]);

        assertEquals(2, getSingleValue(table, "b"));

        values = getValues(table, "c");
        assertEquals(0, values.length);

        TableName tableName1 = TableName.valueOf("_zw_tailers");

        assertTrue(admin.tableExists(tableName1));
    }

    @Test
    public void testSkipUpdates() throws Exception {
        Configuration conf = connection.getConfiguration();
        ConfigUtil.setDefaultFsDirectoryKey(conf, "/tmp/hbase-staging");
        ConfigUtil.setSkipBacklog(conf, true);
        ConfigUtil.setSkipUpdates(conf, true);

        Admin admin = connection.getAdmin();
        assertFalse(admin.tableExists(tableName));

        startTailer(conf);
        createOps();
        stopTailer();

        assertTrue(admin.tableExists(tableName));

        Table table = connection.getTable(tableName);

        assertEquals(2, getCount(table));

        assertEquals(1, getSingleValue(table, "a"));
        assertEquals(2, getSingleValue(table, "b"));

        int[] values = getValues(table, "c");
        assertEquals(0, values.length);
    }

    @Test
    public void testSkipDeletes() throws Exception {

        Configuration conf = connection.getConfiguration();

        ConfigUtil.setDefaultFsDirectoryKey(conf, "/tmp/hbase-staging");

        ConfigUtil.setSkipBacklog(conf, true);
        ConfigUtil.setSkipDeletes(conf, true);

        Admin admin = connection.getAdmin();
        assertFalse(admin.tableExists(tableName));

        startTailer(conf);
        createOps();
        stopTailer();

        assertTrue(admin.tableExists(tableName));

        Table table = connection.getTable(tableName);

        assertEquals(3, getCount(table));

        int[] values = getValues(table, "a");
        assertEquals(2, values.length);
        assertEquals(4, values[0]);
        assertEquals(1, values[1]);

        assertEquals(2, getSingleValue(table, "b"));
        assertEquals(3, getSingleValue(table, "c"));
    }

    @Test
    public void testInsertOnlyWithBufferedWrites() throws Exception {

        Configuration conf = connection.getConfiguration();

        ConfigUtil.setDefaultFsDirectoryKey(conf, "/tmp/hbase-staging");

        ConfigUtil.setSkipBacklog(conf, true);
        ConfigUtil.setSkipUpdates(conf, true);
        ConfigUtil.setSkipDeletes(conf, true);
        ConfigUtil.setBufferWrites(conf, true);

        Admin admin = connection.getAdmin();
        assertFalse(admin.tableExists(tableName));

        startTailer(conf);
        MongoCollection<Document> collection = mongoClient.getDatabase("_test_hbase").getCollection(name);
        collection.insertOne(new Document(new BasicDBObject("_id", "a").append("num", 1)));
        collection.insertOne(new Document(new BasicDBObject("_id", "b").append("num", 2)));
        collection.insertOne(new Document(new BasicDBObject("_id", "c").append("num", 3)));
        Thread.sleep(15000);
        stopTailer();

        assertTrue(admin.tableExists(tableName));

        Table table = connection.getTable(tableName);

        assertEquals(3, getCount(table));

        assertEquals(1, getSingleValue(table, "a"));
        assertEquals(2, getSingleValue(table, "b"));
        assertEquals(3, getSingleValue(table, "c"));
    }

    @Test
    public void testResyncUpdate() throws Exception {

        Configuration conf = connection.getConfiguration();
        ConfigUtil.setDefaultFsDirectoryKey(conf, "/tmp/hbase-staging");

        ConfigUtil.setSkipBacklog(conf, true);

        Admin admin = connection.getAdmin();
        assertFalse(admin.tableExists(tableName));

        startTailer(conf);

        MongoCollection<Document> collection = mongoClient.getDatabase("_test_hbase").getCollection(name);

        collection.insertOne(new Document(new BasicDBObject("_id", "a").append("num", 1)));
        collection.updateOne(
                new BasicDBObject("_id", "a"),
                new BasicDBObject("$set", new BasicDBObject("num", 2)));
        Thread.sleep(15000);
        stopTailer();

        assertTrue(admin.tableExists(tableName));

        Table table = connection.getTable(tableName);

        assertEquals(1, getCount(table));

        int[] values = getValues(table, "a");
        assertEquals(2, values.length);
        assertEquals(2, values[0]);
        assertEquals(1, values[1]);
    }

    private void createOps() throws Exception {
        MongoCollection<Document> collection = mongoClient.getDatabase("_test_hbase").getCollection(name);
        collection.insertOne(new Document(new BasicDBObject("_id", "a").append("num", 1)));
        collection.insertOne(new Document(new BasicDBObject("_id", "b").append("num", 2)));
        collection.insertOne(new Document(new BasicDBObject("_id", "c").append("num", 3)));
        collection.updateOne(new BasicDBObject("_id", "a"), new BasicDBObject("$set", new BasicDBObject("num",4)));
        collection.deleteOne(new BasicDBObject("_id", "c"));
        Thread.sleep(15000);
    }


    private static class TailerThread extends Thread {
        Streamer streamer;

        TailerThread(Configuration conf) throws Exception {
            Connection hbase = ConnectionFactory.createConnection(conf);
            streamer = new Streamer(conf, mongoClient, hbase);
        }

        @Override
        public void run() {
            streamer.stream();
        }
    }

    private void startTailer(Configuration conf) throws Exception {
        tailerThread = new TailerThread(conf);
        tailerThread.start();
        Thread.sleep(2000);
    }

    private void stopTailer() throws Exception {
        tailerThread.interrupt();
        tailerThread.join();
    }

    private int getSingleValue(Table table, String id) throws Exception {
        int[] values = getValues(table, id);
        assertEquals(1, values.length);
        return values[0];
    }

    private int[] getValues(Table table, String id) throws Exception {
        BSONDecoder decoder = new BasicBSONDecoder();
        byte[] raw = DigestUtils.md5(id.getBytes());
        Get get = new Get(raw);
        get.setMaxVersions();
        List<Cell> kvs = table.get(get).getColumnCells("z".getBytes(), "w".getBytes());

        int[] values = new int[kvs.size()];
        for (int i = 0; i < kvs.size(); i++) {
            byte[] bytes = CellUtil.cloneValue(kvs.get(i));
            values[i] = (Integer) decoder.readObject(bytes).get("num");
        }

        return values;
    }

    private int getCount(Table table) throws Exception {
        int count = 0;
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
            count++;
        }

        return count;
    }


}
