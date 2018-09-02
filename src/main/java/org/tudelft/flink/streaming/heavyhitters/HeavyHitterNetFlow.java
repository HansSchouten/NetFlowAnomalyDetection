package org.tudelft.flink.streaming.heavyhitters;

import com.fasterxml.jackson.databind.JsonNode;
import org.tudelft.flink.streaming.NetFlow;
import org.tudelft.flink.streaming.TopN;

import java.util.ArrayList;
import java.util.List;

public class HeavyHitterNetFlow extends NetFlow implements Comparable<HeavyHitterNetFlow> {

    public final int max_size = 10;

    public long count = 1;
    public long total = 0;
    public TopN topNHitters;
    public List<HeavyHitterNetFlow> dataset;

    public HeavyHitterNetFlow() {
        this.topNHitters= new TopN(max_size);
    }

    @Override
    public void setFromString(String line) {
        this.dataset = new ArrayList<>();

        JsonNode jsonDataset = super.getFromString(line);
        if (jsonDataset == null) {
            return;
        }

        for (JsonNode parameters : jsonDataset) {
            HeavyHitterNetFlow flow = new HeavyHitterNetFlow();
            flow.setFromJsonNode(parameters);
            this.dataset.add(flow);
        }
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
