package org.tudelft.flink.streaming.statemachines;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FingerprintMatcher {

    protected Map<String, List<Fingerprint>> fingerprints;
    protected Map<String, Integer> fingerprintSteps;

    public void loadFingerprints() {
        this.fingerprints = new HashMap<>();
        this.fingerprintSteps = new HashMap<>();

        // on binetflow
        //loadFingerprint("output\\fingerprints\\sample1-131\\fingerprint\\dns-traces.txt", "Bubble Dock - DNS");
        //loadFingerprint("output\\fingerprints\\sample1-131\\fingerprint\\host-traces.txt", "Bubble Dock - host");
        //loadFingerprint("output\\fingerprints\\sample2-134\\fingerprint\\dns-traces.txt", "Cryptowall 3.0 - DNS");
        //loadFingerprint("output\\fingerprints\\sample4-221\\fingerprint\\traces.txt", "Locky");

        // emotet
        //loadFingerprint("output\\fingerprints\\emotet\\f1\\traces.txt", "Emotet 1");
        //loadFingerprint("output\\fingerprints\\emotet\\f2\\traces.txt", "Emotet 2");
        //loadFingerprint("output\\fingerprints\\emotet\\f3\\traces.txt", "Emotet 3");

        // kovner
        /*
        loadFingerprint("output\\fingerprints\\5min\\kovner\\f1\\traces.txt", "Kovner 1");

        loadFingerprint("output\\fingerprints\\5min\\135-1-Stlrat-(mixed-3).txt", "Stlrat");
        loadFingerprint("output\\fingerprints\\5min\\320-1-CCleaner-Trojan-udp-1.txt", "CCleaner Trojan - UDP");
        loadFingerprint("output\\fingerprints\\5min\\342-1-Miner-Trojan-udp-1.txt", "Miner Trojan - UDP");
        loadFingerprint("output\\fingerprints\\5min\\locky\\214-1-Locky-(mixed-4)-tcp-1.txt", "Locky - TCP 1");
        loadFingerprint("output\\fingerprints\\5min\\locky\\214-1-Locky-(mixed-4)-tcp-2.txt", "Locky - TCP 2");
        loadFingerprint("output\\fingerprints\\5min\\locky\\214-1-Locky-(mixed-4)-tcp-3.txt", "Locky - TCP 3");
        loadFingerprint("output\\fingerprints\\5min\\locky\\214-1-Locky-(mixed-4)-tcp-3-arp-1.txt", "Locky - ARP");
        loadFingerprint("output\\fingerprints\\5min\\htbot\\348-1-HTBot-tcp-1.txt", "HTBot - TCP 1");
        loadFingerprint("output\\fingerprints\\5min\\htbot\\348-1-HTBot-tcp-2.txt", "HTBot - TCP 2");
        loadFingerprint("output\\fingerprints\\5min\\htbot\\348-1-HTBot-tcp-3.txt", "HTBot - TCP 3");
        loadFingerprint("output\\fingerprints\\5min\\htbot\\348-1-HTBot-tcp-4.txt", "HTBot - TCP 4");
        loadFingerprint("output\\fingerprints\\5min\\adload\\349-1-Adload-udp-1.txt", "Adload - UDP 1");
        loadFingerprint("output\\fingerprints\\5min\\adload\\349-1-Adload-udp-2.txt", "Adload - UDP 2");
        */

        //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-46.165.223.90-TCP-161931.txt", "HTbot - Type I.1 - 46.165.223.90");
        //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-95.215.46.247-TCP-161932.txt", "HTbot - Type I.2 - 95.215.46.247");
            //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-185.86.148.106-TCP-161926.txt", "HTbot - Type II - 185.86.148.106 A - Step 1");
            //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-185.86.148.106-TCP-161927.txt", "HTbot - Type II - 185.86.148.106 A - Step 2");
            //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-185.86.148.106-TCP-161928.txt", "HTbot - Type II - 185.86.148.106 A - Step 3");
        //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-188.0.236.107-TCP-161928.txt", "HTbot - Type IV - 188.0.236.107 - Step 1");
        //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-188.0.236.107-TCP-161930.txt", "HTbot - Type IV - 188.0.236.107 - Step 2");
        //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-188.0.236.107-TCP-161931.txt", "HTbot - Type IV - 188.0.236.107 - Step 3");

            //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-195.113.232.74-TCP-161929.txt", "HTbot - Type I - 195.113.232.74 HTTP A - Step 1");
        //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-195.113.232.74-TCP-161931.txt", "HTbot - Type V.1 - 195.113.232.74 A - Step 2");
            //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-195.113.232.80-TCP-161927.txt", "HTbot - Type I - 195.113.232.80 HTTP A - Step 1");
        //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-195.113.232.80-TCP-161931.txt", "HTbot - Type V.2 - 195.113.232.80 A - Step 2");


        /*
        loadFingerprint("input\\fingerprints\\htbot-19days\\195.113.232.74\\traces-192.168.1.130-195.113.232.74-TCP-day2-9-152030.297.txt", "HTbot - Type I - 195.113.232.74 - Step 1");
        loadFingerprint("input\\fingerprints\\htbot-19days\\195.113.232.74\\traces-192.168.1.130-195.113.232.74-TCP-day2-14-152032.023.txt", "HTbot - Type I - 195.113.232.74 - Step 2");
        loadFingerprint("input\\fingerprints\\htbot-19days\\195.113.232.74\\traces-192.168.1.130-195.113.232.74-TCP-day2-19-152032.712.txt", "HTbot - Type I - 195.113.232.74 - Step 3");

        loadFingerprint("input\\fingerprints\\htbot-19days\\195.113.232.80\\traces-192.168.1.130-195.113.232.80-TCP-day2-10-152032.522.txt", "HTbot - Type I - 195.113.232.80 - Step 1");
        loadFingerprint("input\\fingerprints\\htbot-19days\\195.113.232.80\\traces-192.168.1.130-195.113.232.80-TCP-day2-15-152033.096.txt", "HTbot - Type I - 195.113.232.80 - Step 2");
        loadFingerprint("input\\fingerprints\\htbot-19days\\195.113.232.80\\traces-192.168.1.130-195.113.232.80-TCP-day2-21-152033.643.txt", "HTbot - Type I - 195.113.232.80 - Step 3");
        */

        loadFingerprint("input\\fingerprints\\trickbot-january-1d\\c&c1\\traces-192.168.1.123-37.230.114.93-TCP-day2-6-205430.122.txt", "Trickbot (Jan) 1d");
        loadFingerprint("input\\fingerprints\\trickbot-january-1d\\c&c1\\traces-192.168.1.123-37.230.114.93-TCP-day2-7-205430.254.txt", "Trickbot (Jan) 1d");
        loadFingerprint("input\\fingerprints\\trickbot-january-1d\\c&c1\\traces-192.168.1.123-37.230.114.93-TCP-day2-8-205430.389.txt", "Trickbot (Jan) 1d");
        loadFingerprint("input\\fingerprints\\trickbot-january-1d\\c&c1\\traces-192.168.1.123-37.230.114.93-TCP-day2-9-205432.696.txt", "Trickbot (Jan) 1d");

        loadFingerprint("input\\fingerprints\\emotet-trickbot-june 10h\\c&c1\\traces-10.6.14.101-109.86.227.152-TCP-9-205817.807.txt", "Trickbot (June) 10h");
        loadFingerprint("input\\fingerprints\\emotet-trickbot-june 10h\\c&c1\\traces-10.6.14.101-109.86.227.152-TCP-10-205817.918.txt", "Trickbot (June) 10h");
        loadFingerprint("input\\fingerprints\\emotet-trickbot-june 10h\\c&c1\\traces-10.6.14.101-109.86.227.152-TCP-12-205818.163.txt", "Trickbot (June) 10h");

        loadFingerprint("input\\fingerprints\\malspam-pandabanker-january-16 2h\\c&c1\\traces-10.1.16.101-137.74.150.217-TCP-5-210221.710.txt", "Malspam PandaZeus (January) 2h");
        loadFingerprint("input\\fingerprints\\malspam-pandabanker-january-16 2h\\c&c1\\traces-10.1.16.101-137.74.150.217-TCP-9-210222.157.txt", "Malspam PandaZeus (January) 2h");
        loadFingerprint("input\\fingerprints\\malspam-pandabanker-january-16 2h\\c&c1\\traces-10.1.16.101-137.74.150.217-TCP-9-210222.157.txt", "Malspam PandaZeus (January) 2h");
        loadFingerprint("input\\fingerprints\\malspam-pandabanker-january-16 2h\\c&c1\\traces-10.1.16.101-137.74.150.217-TCP-13-210319.632.txt", "Malspam PandaZeus (January) 2h");

        loadFingerprint("input\\fingerprints\\emotet-pandabanker-january-9 5h\\c&c1\\traces-10.1.9.103-185.125.206.235-TCP-2-211014.970.txt", "Emotet PandaZeus (January) 5h");
        loadFingerprint("input\\fingerprints\\emotet-pandabanker-january-9 5h\\c&c1\\traces-10.1.9.103-185.125.206.235-TCP-3-211015.059.txt", "Emotet PandaZeus (January) 5h");

        loadFingerprint("input\\fingerprints\\emotet-pandabanker-june 5h\\c&c1\\traces-10.6.22.102-91.243.81.13-TCP-2-211552.521.txt", "Emotet PandaZeus (June) 5h");
        loadFingerprint("input\\fingerprints\\emotet-pandabanker-june 5h\\c&c1\\traces-10.6.22.102-91.243.81.13-TCP-3-211607.963.txt", "Emotet PandaZeus (June) 5h");
        loadFingerprint("input\\fingerprints\\emotet-pandabanker-june 5h\\c&c1\\traces-10.6.22.102-91.243.81.13-TCP-4-211608.143.txt", "Emotet PandaZeus (June) 5h");

        loadFingerprint("input\\fingerprints\\zeus-august-2h\\c&c1\\traces-10.8.23.101-185.231.153.228-TCP-6-212232.361.txt", "Zeus (Augustus) 2h");
        loadFingerprint("input\\fingerprints\\zeus-august-2h\\c&c1\\traces-10.8.23.101-185.231.153.228-TCP-8-212232.557.txt", "Zeus (Augustus) 2h");
        loadFingerprint("input\\fingerprints\\zeus-august-2h\\c&c1\\traces-10.8.23.101-185.231.153.228-TCP-9-212232.678.txt", "Zeus (Augustus) 2h");

        loadFingerprint("input\\fingerprints\\vertical-scan\\traces-192.168.2.2-192.168.2.102-TCP-0-233828.963.txt", "Vertical Port Scan");

        System.out.println(this.fingerprints.size() + " fingerprint(s) pre-loaded");
    }

    /**
     * Load a single fingerprint from file.
     *
     * @param path
     * @param name
     */
    protected void loadFingerprint(String path, String name) {
        int step = 1;
        if (this.fingerprints.containsKey(name)) {
            step = this.fingerprints.get(name).size() + 1;
        }

        Fingerprint fingerprint = new Fingerprint(path, name, step);

        if (this.fingerprints.containsKey(name)) {
            this.fingerprints.get(name).add(fingerprint);
        } else {
            List list = new ArrayList<Fingerprint>();
            list.add(fingerprint);
            this.fingerprints.put(name, list);
            this.fingerprintSteps.put(name, 1);
        }
    }

    /**
     * Match the State Machine of the given root state with all loaded fingerprints.
     *
     * @param statemachine
     */
    public void match(StateMachineNetFlow statemachine) {
        for (String name : this.fingerprints.keySet()) {
            int maxStep = Math.min(this.fingerprintSteps.get(name), this.fingerprints.get(name).size());

            for (int step = 1; step <= maxStep; step++) {
                Fingerprint fingerprint = this.fingerprints.get(name).get(step - 1);
                boolean match = fingerprint.match(statemachine.root, statemachine.stateMachineID);
                if (match) {
                    //statemachine.visualiseMatchingMalwareModel();
                    if (step == maxStep) {
                        this.fingerprintSteps.put(name, step + 1);
                    }
                }
            }
        }
    }

}
