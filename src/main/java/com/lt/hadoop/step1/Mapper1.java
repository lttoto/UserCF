package com.lt.hadoop.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by taoshiliu on 2017/10/8.
 */
public class Mapper1 extends Mapper<LongWritable,Text,Text,Text> {

    private Text outKey = new Text();
    private Text outvalue = new Text();

    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {

        String[] values = value.toString().split(",");
        String userID = values[0];
        String itemID = values[1];
        String score = values[2];

        outKey.set(userID);
        outvalue.set(itemID + "_" + score);

        context.write(outKey,outvalue);
    }

}