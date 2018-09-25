package org.tudelft.flink.streaming.statemachines.visualisation;

import java.util.List;

public class PAutomacVisualiser extends StateMachineVisualiser {

    public void visualise(List<Integer> states, int initial, List<List<Integer>> transitions) {
        // add initial state
        this.addState(Integer.toString(initial), "", Integer.toString(initial));
        // add all other states
        for (Integer state : states) {
            this.addState(Integer.toString(state), "", Integer.toString(state));
        }
        // add state transitions
        for (List<Integer> transition: transitions) {
            String fromId = Integer.toString(transition.get(0));
            String label = Integer.toString(transition.get(1));
            String toId = Integer.toString(transition.get(2));
            this.addTransition(fromId, toId, label);
        }
        // write graph to file
        String path = "output\\state-machines\\pautomac.png";
        this.showVisualisation();
        this.writeToFile(path);
    }

}
