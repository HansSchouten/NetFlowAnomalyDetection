package org.tudelft.flink.streaming.statemachines.helpers;

import org.apache.commons.math3.util.Pair;
import org.tudelft.flink.streaming.NetFlow;
import org.tudelft.flink.streaming.statemachines.Symbol;

import java.io.File;
import java.util.*;

public class SymbolConfig {

    public static int alphabet_size = 3;

    protected List<Pair<String, List<Range>>> config;

    /**
     * SymbolConfig constructor.
     */
    public SymbolConfig() {
        this.config = new ArrayList<>();
        // read config file
        try {
            read();
        } catch (Exception ex) {
            System.out.println("Exception while reading Symbol Config:\n" + ex.getMessage());
        }
    }

    /**
     * Read the symbols config file.
     *
     * @throws Exception
     */
    protected void read() throws Exception {
        /*
        String path = "input\\config\\symbols.txt";
        File file = new File(path);
        Scanner sc = new Scanner(file);
        */
        // speedup
        // was 46

        //Scanner sc = new Scanner("packetsize\n0-68 68-129 129-382 382-\n#packets\n0-4 4-8 8-11 11-");
        // port scans
        //Scanner sc = new Scanner("packetsize\n0-68 68-129 129-382 382-\n#packets\n0-2 2-4 4-8 8-11 11-");
        // final
        Scanner sc = new Scanner("packetsize\n0-77 77-148 148-221 221-413 413-\n#packets\n0-2 2-8 8-");

        while (sc.hasNextLine()) {
            // read key line
            String key = sc.nextLine();
            if (sc.hasNextLine()) {
                // read ranges line
                String rangesString = sc.nextLine();
                // get the list with conditions
                ArrayList<Range> value = new ArrayList<>();
                String[] ranges = rangesString.split(" ");
                for (String rangeString : ranges) {
                    Range range = new Range(rangeString);
                    value.add(range);
                }
                // add key and ranges to the config
                this.config.add(new Pair(key, value));
            }
        }
    }

    /**
     * Get the symbol classification of the given NetFlow.
     *
     * @param netFlow
     * @return
     */
    public Symbol getSymbol(NetFlow netFlow) {
        String symbol = "";
        for (Pair<String, List<Range>> pair : this.config) {
            String key = pair.getKey();
            // get value from NetFlow
            Double value = getValue(key, netFlow);
            if (value == null) {
                continue;
            }
            // get the index of the range the value is in
            List<Range> ranges = pair.getValue();
            int range_index = -1;
            for (int i = 0; i < ranges.size(); i++) {
                Range range = ranges.get(i);
                if (range.inRange(value)) {
                    range_index = i;
                    break;
                }
            }
            // add hyphen
            if (!symbol.equals("")) {
                symbol += "-";
            }
            if (range_index >= 0) {
                // add index of the matching range
                symbol += Integer.toString(range_index);
            } else {
                // add special symbol if no range was matched
                symbol += "#";
            }
        }
        return new Symbol(symbol);
    }

    /**
     * Get the long value of the given key for the given NetFlow.
     *
     * @param key
     * @param netFlow
     * @return
     */
    protected Double getValue(String key, NetFlow netFlow) {
        switch (key) {
            case "packetsize":
                return netFlow.averagePacketSize;
            case "#bytes":
                return Double.valueOf(netFlow.byteCount);
            case "#packets":
                return Double.valueOf(netFlow.packetCount);
            case "duration":
                return Double.valueOf(netFlow.end - netFlow.start);
        }
        return null;
    }


    protected class Range {
        protected double low;
        protected double high;

        public Range(String range) {
            String[] parts = range.split("-");
            // set lower bound
            this.low = Integer.parseInt(parts[0]);
            // set upper bound
            if (parts.length == 2) {
                // upper bound is defined
                this.high = Integer.parseInt(parts[1]);
            } else {
                // no upper bound defined
                this.high = Integer.MAX_VALUE;
            }
        }

        /**
         * Return whether the value is in range.
         *
         * @param value
         * @return
         */
        public boolean inRange(double value) {
            return (value >= low && value <= high);
        }

        public String toString() {
            return low + " - " + high;
        }
    }

}
