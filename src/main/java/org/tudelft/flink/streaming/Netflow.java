package org.tudelft.flink.streaming;

import java.io.Serializable;
import com.fasterxml.jackson.databind.JsonNode;

public class Netflow implements Serializable {

    private static final long serialVersionUID = 8078274017619893051L;

    public String json;
    public String srcIP;
    public String dstIP;
    public String srcPort;
    public String dstPort;

    public Netflow(){}

    public void setFromString(String line) {
        JsonNode jsonNode = null;
        // parse JSON
        if (line != null) {
            jsonNode = JSONUtils.convertToJSON(line.getBytes());
        }
        // take NetFLow variables from JSON object
        if (jsonNode != null) {
            this.json = jsonNode.toString();
            JsonNode data = jsonNode.get("DataSets").get(0);
            this.srcIP = data.get(0).get("V").asText();
            this.dstIP = data.get(1).get("V").asText();
            this.srcPort = "80";
            this.dstPort = "80";
        }
    }


    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public String toString() {
        return "<NetFlow[srcIP:" + this.srcIP + ", dstIP:" + this.dstIP + "]>";
    }

}

