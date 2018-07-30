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

    public void checkInfected(HeavyHitterNetFlow newNetflow) {
        List<String> cnc = new ArrayList<>();
        cnc.add("82.165.142.107");
        cnc.add("103.4.18.170");
        cnc.add("81.88.24.211");
        cnc.add("163.172.81.35");
        cnc.add("85.25.119.91");
        cnc.add("8.8.8.8");

        String host = null;
        for (String check : cnc) {
            if (newNetflow.dstIP.equals(check)) {
                host = check;
                break;
            }
        }

        if (host != null) {
            System.out.println(newNetflow.srcIP + " is infected! It communicates with C&C host: " + host);
        }

        this.count++;
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
