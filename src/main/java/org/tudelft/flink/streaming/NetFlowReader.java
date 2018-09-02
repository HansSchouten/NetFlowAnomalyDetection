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

    protected boolean reset_on_end = false;

    protected int resetCount = 0;

    final int MAX_RESETS = 2;

    public enum Format {
        STRATOSPHERE,
        TSHARK,
        NFDUMP,
        CUSTOM_NFDUMP
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
        if (! this.reader.hasNextLine()) {
            if (! this.reset_on_end || this.resetCount == MAX_RESETS) {
                return false;
            }
            this.resetCount++;

            try {
                File file = new File(this.path);
                this.reader = new Scanner(new BufferedReader(new FileReader(file)));
                // skip first line (with columns)
                this.reader.nextLine();
            } catch (Exception e) {
                System.out.println(e.getMessage());
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
        } else if (this.format == Format.NFDUMP) {
            return getNextNFDump();
        } else if (this.format == Format.CUSTOM_NFDUMP) {
            return getNextCustomNFDump();
        }
        return "";
    }

    /**
     * Reset to the start of the file if the end has been reached.
     *
     * @param reset
     */
    public void resetOnEnd(boolean reset) {
        this.reset_on_end = reset;
    }

    public String getNextTShark() {
        JSONObject jsonFlow = new JSONObject();

        String[] args_raw = this.nextLine.split(" ");
        List<String> args = new ArrayList<String>(Arrays.asList(args_raw));
        args.removeAll(Collections.singleton(null));
        args.removeAll(Collections.singleton(""));

        try {
            JSONArray jsonFlowData = new JSONArray();
            jsonFlowData.put(createArg(1, "0x" + Long.toHexString(Long.valueOf(args.get(6)))));
            jsonFlowData.put(createArg(7, args.get(7)));
            jsonFlowData.put(createArg(8, args.get(2)));
            jsonFlowData.put(createArg(11, args.get(8)));
            jsonFlowData.put(createArg(12, args.get(4)));
            jsonFlowData.put(createArg(21, "0"));
            Long val = Long.valueOf(Math.round(Float.valueOf(args.get(1))));
            jsonFlowData.put(createArg(22, val.toString()));

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
            jsonFlowData.put(createArg(1, "0x" + Long.toHexString(Long.valueOf(args[12]))));
            jsonFlowData.put(createArg(2, "0x" + Long.toHexString(Long.valueOf(args[11]))));
            jsonFlowData.put(createArg(7, args[4]));
            jsonFlowData.put(createArg(8, args[3]));
            jsonFlowData.put(createArg(11, args[7]));
            jsonFlowData.put(createArg(12, args[6]));
            jsonFlowData.put(createArg(21, "0"));
            Long val = Long.valueOf(Math.round(Float.valueOf(args[1])));
            jsonFlowData.put(createArg(22, val.toString()));

            JSONArray jsonFlowDataInner = new JSONArray();
            jsonFlowDataInner.put(jsonFlowData);

            jsonFlow.put("DataSets", jsonFlowDataInner);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return "";
        }

        return jsonFlow.toString();
    }

    public String getNextNFDump() {
        JSONObject jsonFlow = new JSONObject();

        String[] args = this.nextLine.split(",");

        try {
            JSONArray jsonFlowData = new JSONArray();
            jsonFlowData.put(createArg(1, "0x" + Long.toHexString(Long.valueOf(args[12]) + Long.valueOf(args[14]) )));
            jsonFlowData.put(createArg(2, "0x" + Long.toHexString(Long.valueOf(args[11]) + Long.valueOf(args[13]) )));
            jsonFlowData.put(createArg(7, args[5]));
            jsonFlowData.put(createArg(8, args[3]));
            jsonFlowData.put(createArg(11, args[6]));
            jsonFlowData.put(createArg(12, args[4]));
            jsonFlowData.put(createArg(21, "0"));
            jsonFlowData.put(createArg(22, "0"));

            JSONArray jsonFlowDataInner = new JSONArray();
            jsonFlowDataInner.put(jsonFlowData);

            jsonFlow.put("DataSets", jsonFlowDataInner);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return "";
        }

        return jsonFlow.toString();
    }

    public String getNextCustomNFDump() {
        JSONObject jsonFlow = new JSONObject();

        String[] args = this.nextLine.split(",");

        try {
            JSONArray jsonFlowData = new JSONArray();
            jsonFlowData.put(createArg(1, "0x" + Long.toHexString(Long.valueOf(args[9]) + Long.valueOf(args[10]) )));
            jsonFlowData.put(createArg(2, "0x" + Long.toHexString(Long.valueOf(args[7]) + Long.valueOf(args[8]) )));
            jsonFlowData.put(createArg(4, getProtocolCode(args[2])));
            jsonFlowData.put(createArg(7, args[4]));
            jsonFlowData.put(createArg(8, args[3]));
            //jsonFlowData.put(createArg(11, args[6]));    // issue in Dport export
            jsonFlowData.put(createArg(12, args[5]));
            jsonFlowData.put(createArg(21, "0"));
            jsonFlowData.put(createArg(22, "0"));

            JSONArray jsonFlowDataInner = new JSONArray();
            jsonFlowDataInner.put(jsonFlowData);

            jsonFlow.put("DataSets", jsonFlowDataInner);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return "";
        }

        return jsonFlow.toString();
    }

    protected JSONObject createArg(int id, String val) throws Exception {
        JSONObject obj = new JSONObject();
        obj.put("I", id);
        obj.put("V", val);
        return obj;
    }

    protected String getProtocolCode(String name) {
        if (name.toUpperCase().equals("UDP")) {
            return "17";
        } else if (name.toUpperCase().equals("TCP")) {
            return "6";
        } else if (name.toUpperCase().equals("ARP")) {
            return "54";
        } else if (name.toUpperCase().contains("ICMP")) {
            return "1";
        } else {
            return "";
        }
    }

}
