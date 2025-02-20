package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;
import java.util.Comparator;

public class TopKReducer extends Reducer<Text, Text, Text, Text> {

    // private PriorityQueue<WordAndCount> pq = new
    // PriorityQueue<WordAndCount>(10);;
    private PriorityQueue<WordAndCount> pq = new PriorityQueue<>(3, Comparator.reverseOrder());

    private Logger logger = Logger.getLogger(TopKReducer.class);

    // public void setup(Context context) {
    //
    // pq = new PriorityQueue<WordAndCount>(10);
    // }

    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * 
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            try {
                double delayRatio = Double.parseDouble(value.toString());

                pq.add(new WordAndCount(new Text(key), delayRatio));

                if (pq.size() > 3) {
                    pq.poll(); // Remove the lowest ratio
                }
            } catch (NumberFormatException e) {
                // Ignore malformed data
            }
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<WordAndCount> values = new ArrayList<>(pq);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());

        // reverse so they are ordered in descending order
        values.sort(Comparator.reverseOrder());

        for (WordAndCount value : values) {
            context.write(value.getWord(), new Text(String.format("%.2f", value.getCount())));
            logger.info("TopKReducer - Top-3 are:  " + value.getWord() + "  Count:" + value.getCount());
        }

    }

}