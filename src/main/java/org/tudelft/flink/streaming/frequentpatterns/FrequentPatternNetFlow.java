package org.tudelft.flink.streaming.frequentpatterns;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.tudelft.flink.streaming.Netflow;
import org.tudelft.flink.streaming.TopN;

public class FrequentPatternNetFlow extends Netflow {

    public CountMinSketch cmSketch;
    public TopN topNPatterns;
    public final int max_size = 10;

    public FrequentPatternNetFlow() {
        this.cmSketch = new CountMinSketch(10, 100, 1);
        this.topNPatterns = new TopN(max_size);
    }

    public void setFromString(String line) {
        super.setFromString(line);
    }

    public void addFlow(FrequentPatternNetFlow newNetflow) {
        this.addPattern("srcIP: " + newNetflow.srcIP + ", dstIP: " + newNetflow.dstIP);
        this.addPattern("srcIP: " + newNetflow.srcIP + ", dstPort: " + newNetflow.dstPort);
        this.addPattern("srcIP: " + newNetflow.srcIP + ", srcPort: " + newNetflow.srcPort);
        this.addPattern("srcIP: " + newNetflow.srcIP + ", srcPort: " + newNetflow.srcPort + ", dstPort: " + newNetflow.dstPort);
    }

    protected void addPattern(String pattern) {
        this.cmSketch.add(pattern, 1);
        long count = this.cmSketch.estimateCount(pattern);
        this.topNPatterns.update(pattern, count);
    }

    public void mergeFrequencies(FrequentPatternNetFlow newHostFrequencies) throws Exception {
        this.cmSketch = CountMinSketch.merge(this.cmSketch, newHostFrequencies.cmSketch);
    }

    public void mergeTopN(FrequentPatternNetFlow newHostFrequencies) throws Exception {
        this.topNPatterns.merge(newHostFrequencies.topNPatterns);
    }

    @Override
    public String toString() {
        String res = "Frequent Patterns\n";
        return res + this.topNPatterns.toString();
    }

}