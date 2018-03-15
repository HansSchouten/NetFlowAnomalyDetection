package org.tudelft.flink.streaming.heavyhitters;

import org.tudelft.flink.streaming.NetFlow;
import org.tudelft.flink.streaming.TopN;

public class HeavyHitterNetFlow extends NetFlow implements Comparable<HeavyHitterNetFlow> {

    public final int max_size = 10;

    public long count = 1;
    public long total = 0;
    public TopN topNHitters;

    public HeavyHitterNetFlow() {
        this.topNHitters= new TopN(max_size);
    }

    public void setFromString(String line) {
        super.setFromString(line);
    }

    public void addHitter(HeavyHitterNetFlow newNetflow) {
        this.count++;
    }

    public void combineCounts(HeavyHitterNetFlow newHostCount) {
        // first iteration, add results of this NetFlow aggregation
        if (this.topNHitters.size() == 0) {
            this.topNHitters.update(this.srcIP, this.count);
            this.total += this.count;
        }

        // add the count of the incoming aggregation
        this.topNHitters.update(newHostCount.srcIP, newHostCount.count);
        this.total += newHostCount.count;
    }

    @Override
    public String toString() {
        String res = "Heavy Hitters\n";
        res += this.topNHitters.toString();
        res += "Total #flows: " + this.total + "\n";
        return res;
    }

    @Override
    public int compareTo(HeavyHitterNetFlow other) {
        if (this.count < other.count) {
            return -1;
        } else if(this.count > other.count) {
            return 1;
        }
        return 0;
    }

}
