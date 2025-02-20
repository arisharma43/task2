package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

public class TopKMapper extends Mapper<Text, Text, Text, Text> {

	private Logger logger = Logger.getLogger(TopKMapper.class);

	private PriorityQueue<WordAndCount> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		try {
			double delayRatio = Double.parseDouble(value.toString());

			// Ensure the queue keeps only the top-3 airlines globally
			pq.add(new WordAndCount(new Text(key), delayRatio));
			if (pq.size() > 3) { // Only keep top 3
				pq.poll();
			}
		} catch (NumberFormatException e) {
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {

		while (pq.size() > 0) {
			WordAndCount wordAndCount = pq.poll();
			context.write(wordAndCount.getWord(), new Text(String.format("%.2f", wordAndCount.getCount())));
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}