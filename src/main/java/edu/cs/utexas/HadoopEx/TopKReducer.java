package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;
import java.util.Comparator;
import java.util.HashMap;

public class TopKReducer extends Reducer<Text, Text, Text, Text> {

    private Logger logger = Logger.getLogger(TopKReducer.class);
    private Map<String, Double> airlineDelayMap = new HashMap<>();
    private PriorityQueue<WordAndCount> pq = new PriorityQueue<>(3, Comparator.reverseOrder());

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            try {
                double delayRatio = Double.parseDouble(value.toString());

                // Update the max delay ratio for each airline
                String airline = key.toString();
                airlineDelayMap.put(airline, Math.max(airlineDelayMap.getOrDefault(airline, 0.0), delayRatio));
            } catch (NumberFormatException e) {
                // Ignore malformed data
            }
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup.");

        // Transfer unique airline data to priority queue
        for (Map.Entry<String, Double> entry : airlineDelayMap.entrySet()) {
            pq.add(new WordAndCount(new Text(entry.getKey()), entry.getValue()));
            if (pq.size() > 3) {
                pq.poll(); // Keep only top-3
            }
        }

        List<WordAndCount> values = new ArrayList<>(pq);
        values.sort(Comparator.reverseOrder()); // Sort descending

        for (WordAndCount value : values) {
            context.write(value.getWord(), new Text(String.format("%.2f", value.getCount())));
        }
    }
}
// public class TopKReducer extends Reducer<Text, Text, Text, Text> {

// // private PriorityQueue<WordAndCount> pq = new
// // PriorityQueue<WordAndCount>(10);;
// private PriorityQueue<WordAndCount> pq = new PriorityQueue<>(3,
// Comparator.reverseOrder());

// private Logger logger = Logger.getLogger(TopKReducer.class);

// // public void setup(Context context) {
// //
// // pq = new PriorityQueue<WordAndCount>(10);
// // }

// /**
// * Takes in the topK from each mapper and calculates the overall topK
// *
// * @param text
// * @param values
// * @param context
// * @throws IOException
// * @throws InterruptedException
// */
// public void reduce(Text key, Iterable<Text> values, Context context) throws
// IOException, InterruptedException {
// for (Text value : values) {
// try {
// double delayRatio = Double.parseDouble(value.toString());

// pq.add(new WordAndCount(new Text(key), delayRatio));

// if (pq.size() > 3) {
// pq.poll(); // Remove the lowest ratio
// }
// } catch (NumberFormatException e) {
// // Ignore malformed data
// }
// }
// }

// public void cleanup(Context context) throws IOException, InterruptedException
// {
// logger.info("TopKReducer cleanup cleanup.");
// logger.info("pq.size() is " + pq.size());

// List<WordAndCount> values = new ArrayList<>(pq);

// while (!pq.isEmpty()) {
// values.add(pq.poll());
// }

// logger.info("values.size() is " + values.size());
// logger.info(values.toString());

// // reverse so they are ordered in descending order
// values.sort(Comparator.reverseOrder());

// for (WordAndCount value : values) {
// context.write(value.getWord(), new Text(String.format("%.2f",
// value.getCount())));
// logger.info("TopKReducer - Top-3 are: " + value.getWord() + " Count:" +
// value.getCount());
// }

// }

// }