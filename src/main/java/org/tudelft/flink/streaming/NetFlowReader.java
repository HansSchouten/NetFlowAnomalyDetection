package org.tudelft.flink.streaming;

import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class NetFlowReader {

    protected BufferedReader reader;

    protected String nextLine;

    public NetFlowReader(String path) throws Exception {
        File file = new File(path);
        this.reader = new BufferedReader(new FileReader(file));
    }

    public boolean hasNext() {
        try {
            this.nextLine = this.reader.readLine();
        } catch (Exception ex) {
            return false;
        }

        return (this.nextLine != null);
    }

    /**
     * Return a NetFlow JSON string of the given binetflow string.
     *
     * @return
     */
    public String getNextJSONFlow() {
        JSONObject jsonFlow = new JSONObject();

        String[] args = this.nextLine.split(",");

        try {
            JSONArray jsonFlowData = new JSONArray();
            jsonFlowData.put(getArg(1, "0x" + Long.toHexString(Long.valueOf(args[12]))));
            jsonFlowData.put(getArg(2, "0x" + Long.toHexString(Long.valueOf(args[11]))));
            jsonFlowData.put(getArg(7, args[4]));
            jsonFlowData.put(getArg(8, args[3]));
            jsonFlowData.put(getArg(11, args[7]));
            jsonFlowData.put(getArg(12, args[6]));
            jsonFlowData.put(getArg(21, "0"));
            Long val = Long.valueOf(Math.round(Float.valueOf(args[1])));
            jsonFlowData.put(getArg(22, val.toString()));

            JSONArray jsonFlowDataInner = new JSONArray();
            jsonFlowDataInner.put(jsonFlowData);

            jsonFlow.put("DataSets", jsonFlowDataInner);
        } catch (Exception ex) {
            return null;
        }

        return jsonFlow.toString();
    }

    protected JSONObject getArg(int id, String val) throws Exception {
        JSONObject obj = new JSONObject();
        obj.put("I", id);
        obj.put("V", val);
        return obj;
    }

}
