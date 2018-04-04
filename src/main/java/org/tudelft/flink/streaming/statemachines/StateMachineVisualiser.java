package org.tudelft.flink.streaming.statemachines;

import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.SingleGraph;

public class StateMachineVisualiser {

    protected Graph graph;

    public void visualise(State root) {
        this.graph = new SingleGraph("State Machine");
        this.graph.addAttribute("ui.antialias");
        this.graph.addAttribute("ui.stylesheet",
                "node { size: 20px; fill-color: rgb(244, 98, 66); text-size: 12; }" +
                        "node.blue { fill-color: rgb(65, 166, 244); }" +
                        "edge { text-size: 12; }"
        );

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
        if (! this.graph.hasNumber(id)) {
            this.graph.addNode(id);
            this.graph.getNode(id).setAttribute("ui.label", "[" + state.count + "]");
            if (state.color == State.Color.BLUE) {
                this.graph.getNode(id).setAttribute("ui.class", "blue");
            }
        }
    }

    public void addEdge(State from, State to, Symbol symbol) {
        String fromId = Integer.toString(from.hashCode());
        String toId = Integer.toString(to.hashCode());
        String edgeId = fromId + "-" + toId;
        if (! this.graph.hasNumber(edgeId)) {
            this.graph.addEdge(edgeId, fromId, toId, true);
            this.graph.getEdge(edgeId).setAttribute("ui.label", symbol.toString());
        }
    }

}
