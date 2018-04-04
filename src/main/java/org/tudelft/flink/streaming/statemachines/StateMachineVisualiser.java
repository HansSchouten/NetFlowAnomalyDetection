package org.tudelft.flink.streaming.statemachines;

import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.SingleGraph;

public class StateMachineVisualiser {

    protected Graph graph;

    public void visualise(State root) {
        this.graph = new SingleGraph("State Machine");
        addState(root);
        this.graph.display();
    }

    public void addState(State state) {
        addNode(state);
        for (Symbol symbol : state.getTransitions()) {
            State next = state.getState(symbol);
            addState(next);
            addEdge(state, next, symbol);
        }
    }

    public void addNode(State state) {
        String id = Integer.toString(state.hashCode());
        if (! graph.hasNumber(id)) {
            graph.addNode(id);
            graph.getNode(id).setAttribute("ui.label", "[" + state.count + "]");
        }
    }

    public void addEdge(State from, State to, Symbol symbol) {
        String fromId = Integer.toString(from.hashCode());
        String toId = Integer.toString(to.hashCode());
        String edgeId = fromId + "-" + toId;
        if (! graph.hasNumber(edgeId)) {
            graph.addEdge(edgeId, fromId, toId, true);
            graph.getEdge(edgeId).setAttribute("ui.label", symbol.toString());
        }
    }

}
