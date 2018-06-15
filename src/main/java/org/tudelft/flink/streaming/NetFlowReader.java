package org.tudelft.flink.streaming;

import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class NetFlowReader {

    protected Scanner reader;

    protected String nextLine;

    protected Format format;

    protected String path;

    public enum Format {
        STRATOSPHERE,
        TSHARK
    }

    public NetFlowReader(String path, Format format) throws Exception {
        this.path = path;
        this.format = format;
        File file = new File(path);
        this.reader = new Scanner(new BufferedReader(new FileReader(file)));
        // skip first line (with columns)
        this.reader.nextLine();
    }

    public boolean hasNext() {
        if (!this.reader.hasNextLine()) {
            try {
                File file = new File(this.path);
                this.reader = new Scanner(new BufferedReader(new FileReader(file)));
                // skip first line (with columns)
                this.reader.nextLine();
            } catch (Exception e) {
                return false;
            }
        }
        return this.reader.hasNextLine();
    }

    /**
     * Return a NetFlow JSON string of the given binetflow string.
     *
     * @return
     */
    public String getNextJSONFlow() {
        this.nextLine = this.reader.nextLine();

        if (this.format == Format.STRATOSPHERE) {
            return getNextStratosphere();
        } else if (this.format == Format.TSHARK) {
            return getNextTShark();
        }
        return "";
    }

    public String getNextTShark() {
        JSONObject jsonFlow = new JSONObject();

        String[] args_raw = this.nextLine.split(" ");
        List<String> args = new ArrayList<String>(Arrays.asList(args_raw));
        args.removeAll(Collections.singleton(null));
        args.removeAll(Collections.singleton(""));

        try {
            JSONArray jsonFlowData = new JSONArray();
            jsonFlowData.put(getArg(1, "0x" + Long.toHexString(Long.valueOf(args.get(6)))));
            jsonFlowData.put(getArg(7, args.get(7)));
            jsonFlowData.put(getArg(8, args.get(2)));
            jsonFlowData.put(getArg(11, args.get(8)));
            jsonFlowData.put(getArg(12, args.get(4)));
            jsonFlowData.put(getArg(21, "0"));
            Long val = Long.valueOf(Math.round(Float.valueOf(args.get(1))));
            jsonFlowData.put(getArg(22, val.toString()));

            JSONArray jsonFlowDataInner = new JSONArray();
            jsonFlowDataInner.put(jsonFlowData);

            jsonFlow.put("DataSets", jsonFlowDataInner);
        } catch (Exception ex) {
            return "";
        }

        return jsonFlow.toString();
    }

    public String getNextStratosphere() {
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
