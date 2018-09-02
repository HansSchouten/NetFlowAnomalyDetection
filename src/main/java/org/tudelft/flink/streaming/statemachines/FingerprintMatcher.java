package org.tudelft.flink.streaming.statemachines;

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
     * @param root
     * @param stateMachineID
     */
    public void match(State root, String stateMachineID) {
        for (Fingerprint fingerprint : this.fingerprints) {
            boolean match = fingerprint.match(root, stateMachineID);
        }
    }

}
