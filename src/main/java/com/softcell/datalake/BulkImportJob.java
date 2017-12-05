package com.softcell.datalake;


import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.UUID;

public class BulkImportJob {

    private final Job _job;

    private final String _uuid;

    private final MongoClientURI _mongoURI;

    private final Path _hfilePath;

    private final String _tableName;

    public BulkImportJob(Configuration conf, MongoClientURI mongoURI, String tableName) throws IOException {
        Configuration cloned = new Configuration(conf);
        _uuid = UUID.randomUUID().toString();

        _job = Job.getInstance(cloned, tableName + "." + _uuid);

        _mongoURI = mongoURI;
        _tableName = tableName;

        String tmpPath = ConfigUtil.getTemporaryHFilePath(conf);
        _hfilePath = new Path(tmpPath, _uuid);

        setupJob();
    }

    public BulkImportJob(Job _job, String _uuid, MongoClientURI _mongoURI, Path _hfilePath, String _tableName) {
        this._job = _job;
        this._uuid = _uuid;
        this._mongoURI = _mongoURI;
        this._hfilePath = _hfilePath;
        this._tableName = _tableName;
    }

    public Configuration getConfiguration() {
        return _job.getConfiguration();
    }

    public Job getJob() {
        return _job;
    }

    public MongoClientURI getMongoURI() {
        return _mongoURI;
    }

    public String getOutputTableName() {
        return _tableName;
    }

    public Path getHFilePath() {
        return _hfilePath;
    }

    public boolean run() throws Exception {
        configureBulkImport();

        boolean success = _job.waitForCompletion(true);

        if (success) {
            completeImport();
        }

        return success;
    }

    public void submit() throws Exception {
        configureBulkImport();
        _job.submit();
    }

    public void completeImport() throws Exception {
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConfiguration());

        Connection connection = ConnectionFactory.createConnection(getConfiguration());

        Table table = connection.getTable(TableName.valueOf(_tableName));

        Admin admin = connection.getAdmin();

        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(_tableName));

        loader.doBulkLoad(_hfilePath, admin, table, regionLocator);

        FileSystem fs = _hfilePath.getFileSystem(getConfiguration());
        fs.delete(_hfilePath, true);
    }

    private void setupJob() {
        _job.setInputFormatClass(MongoInputFormat.class);
        _job.setMapperClass(BulkImportMapper.class);
        _job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        _job.setMapOutputValueClass(Put.class);
        MongoConfigUtil.setInputURI(getConfiguration(), _mongoURI);
        MongoConfigUtil.setReadSplitsFromSecondary(getConfiguration(), true);
    }

    private void configureBulkImport() throws IOException {

        Connection connection = ConnectionFactory.createConnection();

        Table table = connection.getTable(TableName.valueOf(_tableName));

        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(_tableName));

        HFileOutputFormat2.configureIncrementalLoad(_job, table, regionLocator);

        HFileOutputFormat2.setOutputPath(_job, _hfilePath);

    }


}
