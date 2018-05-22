package org.tudelft.flink.streaming.statemachines.validation;

public class RunValidation {

    public static void main(String[] args) throws Exception {
        visualisePAutomac();
    }

    public static void visualisePAutomac() throws Exception {
        String path = "input\\pautomac\\set-I\\0-1pautomac_model.txt";
        VisualisePAutomac visualiser = new VisualisePAutomac();
        visualiser.visualise(path);
    }

}
