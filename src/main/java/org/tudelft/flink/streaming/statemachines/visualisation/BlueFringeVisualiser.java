package org.tudelft.flink.streaming.statemachines.visualisation;

import org.tudelft.flink.streaming.statemachines.State;
import org.tudelft.flink.streaming.statemachines.Symbol;

import java.util.List;

public class BlueFringeVisualiser extends StateMachineVisualiser {

    protected boolean only_red;

    public BlueFringeVisualiser() {
        this.only_red = false;
    }

    public BlueFringeVisualiser(boolean only_red) {
        this.only_red = only_red;
    }

    /**
     * Create a State Machine visualisation with the given root node.
     *
     * @param redStates
     */
    public void visualise(List<State> redStates) {
        // add all red states
        for (State state : redStates) {
            addState(state);
        }
        // add all blue states and edges from each red state
        for (State state : redStates) {
            if (! this.only_red) {
                addBlueStates(state);
            }
            addTransitions(state);
        }
    }

    /**
     * Write the visualisation to file.
     *
     * @param stateMachineID
     */
    public void writeToFile(String stateMachineID) {
        // get file path
        String cleanID = stateMachineID.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
        String path = "output\\state-machines\\machine-" + cleanID + ".png";
        // write the graph to file
        super.writeToFile(path);
    }

    /**
     * Add a node for the given state.
     *
     * @param state
     */
    protected void addState(State state) {
        // skip empty states
        if (state.getCount() == 0) {
            return;
        }
        // define the id of this state
        String id = Integer.toString(state.hashCode());
        // define css class
        String cssClass = "";
        if (state.getColor() == State.Color.BLUE) {
            cssClass = "blue";
        }
        // define the label
        String label = "[" + state.getCount() + "]";
        // add the node to the graph
        super.addState(id, cssClass, label);
    }

    /**
     * Add all blue states linked to by the given state.
     *
     * @param state
     */
    protected void addBlueStates(State state) {
        for (Symbol symbol : state.getTransitions()) {
            State next = state.getState(symbol);
            if (next.getColor() == State.Color.BLUE) {
                this.addState(next);
            }
        }
    }

    /**
     * Add an edge between the given states with the given symbol.
     *
     * @param origin
     */
    protected void addTransitions(State origin) {
        for (Symbol symbol : origin.getTransitions()) {
            String fromId = Integer.toString(origin.hashCode());
            State to = origin.getState(symbol);
            if (to.getColor() == State.Color.BLUE && this.only_red) {
                continue;
            }
            String label = symbol.toString();
            String toId = Integer.toString(to.hashCode());
            super.addTransition(fromId, toId, label);
        }
    }

}
