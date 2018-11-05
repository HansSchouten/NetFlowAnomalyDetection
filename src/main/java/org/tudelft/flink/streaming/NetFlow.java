package org.tudelft.flink.streaming;

import java.io.Serializable;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;

public class NetFlow implements Serializable {

    private static final long serialVersionUID = 8078274017619893051L;

    public String json;

    public long byteCount;
    public long packetCount;
    public double averagePacketSize;
    public double duration;
    public double interarrivalTime;
    public Protocol protocol = Protocol.UNKNOWN;
    public String srcIP;
    public String dstIP;
    public Integer srcPort;
    public Integer dstPort;
    public String symbol;
    public long start;
    public long end;
    public boolean lastFlow = false; //indicates that the last flow of a test sequence has passed
    public String combination; //used to pass which parameters to use (for evaluation purposes)
    /**
     * Parameters to group on.
     */
    public String sourceIP;
    public String IPPair;
    public String IPPairProtocol;
    public String datasetLabel; // used for testing performance
    public String group;
    public String all = "";


    public enum Protocol {
        UDP,
        TCP,
        ARP,
        ICMP,
        OTHER,
        UNKNOWN
    }

    /**
     * Set all instance variables to the values encoded in the given JSON string.
     * @param line
     */
    public void setFromString(String line) {
    }

    public JsonNode getFromString(String data) {
        JsonNode jsonNode = null;

        // parse JSON
        if (data != null) {
            jsonNode = JSONUtils.convertToJSON(data.getBytes());
        }
        // take NetFLow variables from JSON object
        if (jsonNode != null) {
            /*
            int count = Integer.parseInt(jsonNode.get("Header").get("SeqNum").toString());
            //System.out.println(count);
            if (count == 0) {
                System.out.println(System.currentTimeMillis());
            }
            */


            this.json = jsonNode.toString();
            return jsonNode.get("DataSets");
        }
        return null;
    }

    public void setFromJsonNode(JsonNode parameters) {
        // Store all IPFIX Information Elements in this NetFlow object
        for (JsonNode parameter : parameters) {
            Integer id = Integer.parseInt(parameter.get("I").asText());
            String value = parameter.get("V").asText();
            storeElement(id, value);
        }

        /*
        // store ip pair (with lowest ip first)
        if (this.srcIP.hashCode() < this.dstIP.hashCode()) {
            this.IPPair = this.srcIP + "," + this.dstIP;
        } else {
            this.IPPair = this.dstIP + "," + this.srcIP;
        }
        */
        this.sourceIP = this.srcIP;
        this.IPPair = this.srcIP + "," + this.dstIP;
        this.IPPairProtocol = this.IPPair + "," + this.protocol.toString();// + ",day" + this.start;
        // collect all flows in one stream for debugging
        //this.IPPairProtocol = "DEBUG";
        /*
        if (this.dstPort != 443 && this.srcPort != 443) {
            this.IPPairProtocol = "non-http";
            this.protocol = Protocol.OTHER;
        }
        */
        /*
        if (this.combination != null) {
            this.group = this.datasetLabel + "-" + this.combination;
        }
        */
        /*
        if (this.protocol != Protocol.TCP && this.protocol != Protocol.UDP) {
            this.IPPairProtocol = "non-http";
            this.protocol = Protocol.OTHER;
        }
        */

        // compute average packet size
        this.averagePacketSize = 0;
        if (this.packetCount > 0) {
            this.averagePacketSize = this.byteCount / (double) this.packetCount;
        }

        this.duration = 0;
        this.interarrivalTime = 0;
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
            case 1:
                this.byteCount = Long.decode(value);
                break;
            case 2:
                this.packetCount = Long.decode(value);
                break;
            case 4:
                if (value.equals("17")) {
                    this.protocol = Protocol.UDP;
                } else if (value.equals("6")) {
                    this.protocol = Protocol.TCP;
                } else if (value.equals("1")) {
                    this.protocol = Protocol.ICMP;
                } else if (value.equals("54") || value.equals("91")) {
                    this.protocol = Protocol.ARP;
                } else {
                    this.protocol = Protocol.OTHER;
                }
                break;
            case 7:
                this.srcPort = Integer.parseInt(value);
                break;
            case 8:
                this.srcIP = value;
                break;
            case 11:
                this.dstPort = Integer.parseInt(value);
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

            // test/design purposes
            case -3:
                this.combination = value;
                break;
            case -2:
                if (value.equals("1")) {
                    this.lastFlow = true;
                }
                break;
            case -1:
                this.datasetLabel = value;
                break;
            case 0:
                this.symbol = value;
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
                + ", start:"  + this.start
                + ", end:"  + this.end
                + "]>";
    }

}

