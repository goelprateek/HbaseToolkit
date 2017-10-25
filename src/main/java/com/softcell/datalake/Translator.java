package com.softcell.datalake;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.bson.BSONObject;

public interface Translator {

    String mapNamespaceToHBaseTable(String database, String collection);

    HTableDescriptor describeHBaseTable(String tableName);

    byte[] createRowKey(BSONObject bsonObject);

    Put createPut(byte[] row , BSONObject bsonObject);
}
