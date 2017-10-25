package com.softcell.datalake;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.softcell.datalake.utils.HBaseHelper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.bson.BSONDecoder;
import org.bson.BasicBSONDecoder;
import org.bson.Document;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class BulkImportRunnerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportRunnerTest.class);

    private static  HBaseCommonTestingUtility util = new HBaseCommonTestingUtility();

    private static MongoClient mongoClient;

    private static Connection connection;

    @BeforeClass
    public static void setUpClass(){

        try{

            mongoClient = new MongoClient("localhost",27017);
            mongoClient.getUsedDatabases();

        }catch (Exception e){
            LOGGER.error(" Not able to connect with mongo instance with probable cause {}",e);
            assumeTrue(false);
        }

    }


    @Before
    public void setup(){
        try {

            Configuration configuration = util.getConfiguration();

            connection = ConnectionFactory.createConnection(configuration);

            mongoClient.getDatabase("_test_hbase").drop();

            MongoCollection<Document> fooCollection = mongoClient.getDatabase("_test_hbase")
                    .getCollection("foo");

            fooCollection.insertOne(new Document(new BasicDBObject("_id", "a").append("num", 1)));
            fooCollection.insertOne(new Document(new BasicDBObject("_id", "b").append("num", 2)));
            fooCollection.insertOne(new Document(new BasicDBObject("_id", "c").append("num", 3)));


            MongoCollection<Document> barCollection = mongoClient.getDatabase("_test_zerowing")
                    .getCollection("bar");

            barCollection.insertOne(new Document(new BasicDBObject("_id", "a").append("num", 11)));
            barCollection.insertOne(new Document(new BasicDBObject("_id", "b").append("num", 12)));
            barCollection.insertOne(new Document(new BasicDBObject("_id", "c").append("num", 13)));


        } catch(Exception e) {
            System.out.println("error setting up mongo: " + e);
        }
    }

    @After
    public void tearDown() throws Exception {
        try {

            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("zw._test_hbase.foo");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);

        } catch (TableNotFoundException e) {}

        try {

            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("zw._test_hbase.bar");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);

        } catch (TableNotFoundException e) {}
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        connection.close();
    }

    @Test
    public void testBasicImport() throws Exception {

        Configuration conf = connection.getConfiguration();

        TableName fooTable = TableName.valueOf("zw._test_hbase.foo");

        TableName barTable = TableName.valueOf("zw._test_hbase.bar");

        BulkImportRunner runner = new BulkImportRunner(conf);

        runner.addJob("mongodb://localhost:27017/_test_hbase.foo");

        assertTrue(runner.doBulkImport());

        Admin admin = connection.getAdmin();

        assertTrue(admin.tableExists(fooTable));
        assertFalse(admin.tableExists(barTable));

        Table table = connection.getTable(fooTable);

        assertEquals(3, getCount(table));
        assertEquals(1, getSingleValue(table, "a"));
        assertEquals(2, getSingleValue(table, "b"));
        assertEquals(3, getSingleValue(table, "c"));
    }

    @Test
    public void testExpandDatabase() throws Exception {
        Configuration conf = connection.getConfiguration();

        BulkImportRunner runner = new BulkImportRunner(conf);
        runner.addJobsForNamespace("mongodb://localhost:27017/", "_test_hbase", null);
        assertTrue(runner.doBulkImport());

        Admin admin = connection.getAdmin();

        TableName fooTableName = TableName.valueOf("zw._test_hbase.foo");

        TableName barTableName = TableName.valueOf("zw._test_hbase.bar");

        assertTrue(admin.tableExists(fooTableName));
        assertTrue(admin.tableExists(barTableName));
    }

    @Test
    public void testMerge() throws Exception {
        Configuration conf = util.getConfiguration();

        BulkImportRunner runner = new BulkImportRunner(conf);
        runner.addJob("mongodb://localhost:27017/_test_hbase.foo");
        assertTrue(runner.doBulkImport());

        Table table = connection.getTable(TableName.valueOf("zw._test_hbase.foo"));

        assertEquals(3, getCount(table));
        assertEquals(1, getSingleValue(table, "a"));
        assertEquals(2, getSingleValue(table, "b"));
        assertEquals(3, getSingleValue(table, "c"));

        MongoCollection<Document> collection = mongoClient.getDatabase("_test_hbase").getCollection("foo");

        collection.insertOne(new Document(new BasicDBObject("_id", "d").append("num", 4)));
        collection.insertOne(new Document(new BasicDBObject("_id", "e").append("num", 5)));
        collection.insertOne(new Document(new BasicDBObject("_id", "a").append("num", 6)));

        ConfigUtil.setMergeExistingTable(conf, true);

        runner = new BulkImportRunner(conf);
        runner.addJob("mongodb://localhost:27017/_test_hbase.foo");
        assertTrue(runner.doBulkImport());

        //a should have the old value stored under the new one
        int[] values = getValues(table, "a");
        assertEquals(2, values.length);
        assertEquals(6, values[0]);
        assertEquals(1, values[1]);

        values = getValues(table, "b");
        assertEquals(2, values.length);
        assertEquals(2, values[0]);
        assertEquals(2, values[1]);

        values = getValues(table, "c");
        assertEquals(2, values.length);
        assertEquals(3, values[0]);
        assertEquals(3, values[1]);

        assertEquals(4, getSingleValue(table, "d"));
        assertEquals(5, getSingleValue(table, "e"));

        assertEquals(5, getCount(table));
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
        for (Result rs = scanner.next(); rs != null; rs = scanner.next()) { count++; }

        return count;
    }



}
