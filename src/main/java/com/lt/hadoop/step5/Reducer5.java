package com.lt.hadoop.step5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by taoshiliu on 2017/10/8.
 */
public class Reducer5 extends Reducer<Text,Text,Text,Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {

        StringBuilder sb = new StringBuilder();
        for(Text value:values) {
            sb.append(value + ",");
        }

        String line = null;
        if(sb.toString().endsWith(",")) {
            line = sb.substring(0,sb.length() - 1);
        }

        outKey.set(key);
        outValue.set(line);
        context.write(outKey,outValue);
    }
}
