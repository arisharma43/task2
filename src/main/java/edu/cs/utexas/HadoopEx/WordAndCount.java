package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordAndCount implements Comparable<WordAndCount> {

    private final Text word;
    private final double count;

    public WordAndCount(Text word, double count) {
        this.word = word;
        this.count = count;
    }

    public Text getWord() {
        return word;
    }

    public double getCount() {
        return count;
    }

    @Override
    public int compareTo(WordAndCount other) {
        // Sort in DESCENDING order
        return Double.compare(this.count, other.count);
    }

    @Override
    public String toString() {
        return "(" + word.toString() + " , " + count + ")";
    }
}