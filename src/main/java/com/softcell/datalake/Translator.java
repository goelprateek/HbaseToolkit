package com.softcell.datalake;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.bson.BSONObject;
import org.bson.Document;
import org.json.simple.parser.ParseException;

public interface Translator {

    String mapNamespaceToHBaseTable(String database, String collection);

    HTableDescriptor describeHBaseTable(String tableName);

    byte[] createRowKey(Document bsonObject);

    Put createPut(byte[] row , Document bsonObject) ;
}
