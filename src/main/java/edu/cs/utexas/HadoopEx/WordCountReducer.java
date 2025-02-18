package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

    // public void reduce(Text text, Iterable<IntWritable> values, Context context)
    // throws IOException, InterruptedException {

    // int sum = 0;

    // for (IntWritable value : values) {
    // sum += value.get();
    // }

    // context.write(text, new IntWritable(sum));
    // }
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totalDelay = 0;
        int totalFlights = 0;

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            totalDelay += Integer.parseInt(parts[0]); // Sum of delays
            totalFlights += Integer.parseInt(parts[1]); // Count flights
        }

        if (totalFlights > 0) {
            context.write(key, new Text(String.format("%.2f", (double) totalDelay / totalFlights)));
        }
    }
}