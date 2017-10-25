package com.softcell.datalake;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.types.ObjectId;

public class BasicTranslator implements Translator {


    public final byte[] COL_FAMILY = "z".getBytes();

    public final byte[] COL_QUALIFIER = "w".getBytes();

    public final BasicBSONEncoder bsonEncoder = new BasicBSONEncoder();


    @Override
    public String mapNamespaceToHBaseTable(String database, String collection) {
        return "zw." + database + "." + collection;
    }

    @Override
    public HTableDescriptor describeHBaseTable(String tableName) {

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

        HColumnDescriptor familyDesc = new HColumnDescriptor(COL_FAMILY);

        tableDesc.addFamily(familyDesc);

        return tableDesc;
    }

    @Override
    public byte[] createRowKey(BSONObject bsonObject) {

        Object id = bsonObject.get("_id");

        byte[] raw;

        if (id instanceof ObjectId) {
            raw = ((ObjectId) id).toByteArray();
        } else if (id instanceof String) {
            raw = ((ObjectId) id).toByteArray();
        } else if (id instanceof BSONObject) {
            raw = bsonEncoder.encode((BSONObject) id);
        } else {
            throw new RuntimeException(" not able to serialize _id: " + id.toString());
        }

        return DigestUtils.md5(raw);
    }

    @Override
    public Put createPut(byte[] row, BSONObject bsonObject) {

        byte[] raw = bsonEncoder.encode(bsonObject);

        Put  put = new Put(row);

        put.addColumn(COL_FAMILY,COL_QUALIFIER,raw);

        return put;
    }
}
