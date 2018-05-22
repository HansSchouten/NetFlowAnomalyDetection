package org.tudelft.flink.streaming;

import java.io.Serializable;

import com.fasterxml.jackson.databind.JsonNode;

public class NetFlow implements Serializable {

    private static final long serialVersionUID = 8078274017619893051L;

    public String json;

    public long byteCount;
    public long packetCount;
    public String srcIP;
    public String dstIP;
    public String srcPort;
    public String dstPort;
    public String IPPair;
    public String symbol;
    public long start;
    public long end;

    /**
     * Set all instance variables to the values encoded in the given JSON string.
     * @param data
     */
    public void setFromString(String data) {
        JsonNode jsonNode = null;

        // parse JSON
        if (data != null) {
            jsonNode = JSONUtils.convertToJSON(data.getBytes());
        }
        // take NetFLow variables from JSON object
        if (jsonNode != null) {
            this.json = jsonNode.toString();
            JsonNode parameters = jsonNode.get("DataSets").get(0);

            // Store all IPFIX Information Elements in this NetFlow object
            for (JsonNode parameter : parameters) {
                Integer id = Integer.parseInt(parameter.get("I").asText());
                String value = parameter.get("V").asText();
                storeElement(id, value);
            }

            // store ip pair (with lowest ip first)
            if (this.srcIP.hashCode() < this.dstIP.hashCode()) {
                this.IPPair = this.srcIP + "," + this.dstIP;
            } else {
                this.IPPair = this.dstIP + "," + this.srcIP;
            }
        }
    }

    /**
     * Set the instance variable corresponding to the given IPFIX Element ID.
     * see: https://www.iana.org/assignments/ipfix/ipfix.xhtml
     *
     * @param id        the Element ID
     * @param value     the value
     */
    protected void storeElement(Integer id, String value) {
        switch (id) {
            case 0:
                this.symbol = value;
                break;
            case 1:
                this.byteCount = Long.decode(value);
                break;
            case 2:
                this.packetCount = Long.decode(value);
                break;
            case 7:
                this.srcPort = value;
                break;
            case 8:
                this.srcIP = value;
                break;
            case 11:
                this.dstPort = value;
                break;
            case 12:
                this.dstIP = value;
                break;
            case 21:
                this.start = Long.valueOf(value);
                break;
            case 22:
                this.end = Long.valueOf(value);
                break;
        }
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj){
            return true;
        }
        if (obj == null){
            return false;
        }
        if (getClass() != obj.getClass()){
            return false;
        }
        NetFlow other = (NetFlow) obj;
        return this.toString().equals(other.toString());
    }

    @Override
    public String toString() {
        return "<NetFlow[srcIP:" + this.srcIP
                + ", srcPort:"  + this.srcPort
                + ", dstIP:" + this.dstIP
                + ", dstPort:"  + this.dstPort
                + ", byteCount:"  + this.byteCount
                + ", packetCount:"  + this.packetCount
                + "]>";
    }

}

