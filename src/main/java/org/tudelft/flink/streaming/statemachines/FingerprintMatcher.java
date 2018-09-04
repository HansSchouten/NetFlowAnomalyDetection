package org.tudelft.flink.streaming.statemachines;

import org.apache.hadoop.yarn.state.StateMachine;

import java.util.ArrayList;
import java.util.List;

public class FingerprintMatcher {

    protected List<Fingerprint> fingerprints;

    public void loadFingerprints() {
        this.fingerprints = new ArrayList<>();

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

        loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-195.113.232.74-TCP-161929.txt", "HTbot - Type I - 195.113.232.74 HTTP A - Step 1");
        //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-195.113.232.74-TCP-161931.txt", "HTbot - Type V.1 - 195.113.232.74 A - Step 2");
        loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-195.113.232.80-TCP-161927.txt", "HTbot - Type I - 195.113.232.80 HTTP A - Step 1");
        //loadFingerprint("input\\fingerprints\\htbot-19days\\traces-192.168.1.130-195.113.232.80-TCP-161931.txt", "HTbot - Type V.2 - 195.113.232.80 A - Step 2");

        System.out.println(this.fingerprints.size() + " fingerprint(s) loaded");
    }

    /**
     * Load a single fingerprint from file.
     *
     * @param path
     * @param name
     */
    protected void loadFingerprint(String path, String name) {
        try {
            Fingerprint fingerprint = new Fingerprint();
            fingerprint.loadFingerprint(path, name);
            this.fingerprints.add(fingerprint);
        } catch (Exception  ex) {
            System.out.println("Error while loading fingerprint: " + ex.getMessage());
        }
    }

    /**
     * Match the State Machine of the given root state with all loaded fingerprints.
     *
     * @param statemachine
     */
    public void match(StateMachineNetFlow statemachine) {
        for (Fingerprint fingerprint : this.fingerprints) {
            try {
                boolean match = fingerprint.match(statemachine.root, statemachine.stateMachineID);
                if (match) {
                    statemachine.visualiseMalwareModel();
                }
            } catch (Exception ex) {
            }
        }
    }

}
