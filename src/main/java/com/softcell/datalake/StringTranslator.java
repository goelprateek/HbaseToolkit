package com.softcell.datalake;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.BSONEncoder;
import org.bson.BasicBSONEncoder;
import org.bson.Document;

public class StringTranslator implements Translator{

    public final byte[] COL_FAMILY = "colfam1".getBytes();

    public final byte[] COL_QUALIFIER = "col1".getBytes();

    public final BSONEncoder bsonEncoder = new BasicBSONEncoder();


    @Override
    public String mapNamespaceToHBaseTable(String database, String collection) {
        return "mongo." + database + "." + collection;
    }

    @Override
    public HTableDescriptor describeHBaseTable(String tableName) {

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

        HColumnDescriptor familyDesc = new HColumnDescriptor(COL_FAMILY);

        tableDesc.addFamily(familyDesc);

        return tableDesc;
    }

    @Override
    public byte[] createRowKey(Document bsonObject) {

        Object id = bsonObject.get("_id");

        return id.toString().getBytes();
    }

    @Override
    public Put createPut(byte[] row, Document bsonObject) {

        Put put = new Put(row);

        bsonObject.keySet()
                .parallelStream()
                .forEachOrdered(obj -> put.addColumn(COL_FAMILY, Bytes.toBytes(obj), Bytes.toBytes(bsonObject.get(obj).toString())));

        return put;

    }
}
