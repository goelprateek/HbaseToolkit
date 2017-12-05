package com.softcell.datalake;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Set;

public class BasicTranslator implements Translator {


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

        byte[] raw;

        if (id instanceof ObjectId) {
            raw = ((ObjectId) id).toByteArray();
        } else if (id instanceof String) {
            raw = ((String) id).getBytes();
        } else if (id instanceof Document) {
            raw = bsonEncoder.encode((BSONObject) id);
        } else {
            throw new RuntimeException(" not able to serialize _id: " + id.toString());
        }

        return raw;//DigestUtils.md5(raw);
    }

    @Override
    public Put createPut(byte[] row, Document bsonObject) {

        Put put = new Put(new ObjectId(row).toByteArray());

        bsonObject.keySet()
                .parallelStream()
                .forEachOrdered(obj -> put.addColumn(COL_FAMILY, Bytes.toBytes(obj), toByteArray(bsonObject.get(obj))));

        return put;
    }

    private byte[] toByteArray(Object o) {
        byte[] bytes = null;


        try (ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream outputStream = new ObjectOutputStream(arrayOutputStream)) {

            outputStream.writeObject(o);
            outputStream.flush();
            bytes = arrayOutputStream.toByteArray();


        } catch (Exception e) {
            e.printStackTrace();
        }

        return bytes;
    }


}
