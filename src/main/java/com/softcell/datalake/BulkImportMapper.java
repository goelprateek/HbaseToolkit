package com.softcell.datalake;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

public class BulkImportMapper<T extends Translator> extends Mapper<Object, BSONObject, ImmutableBytesWritable, Put> {

    private Translator _translator;

    public void setup(Context context) {
        _translator = ConfigUtil.getTranslator(context.getConfiguration());
    }

    public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
        byte[] row = _translator.createRowKey(value);
        Put put = _translator.createPut(row, value);

        ImmutableBytesWritable outKey = new ImmutableBytesWritable(row);
        context.write(outKey, put);
    }

}
