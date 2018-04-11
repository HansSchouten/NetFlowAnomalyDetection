package org.tudelft.flink.streaming.statemachines;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.ui.swingViewer.Viewer;

import java.util.*;

public class StateMachineVisualiser {

    public boolean use_numbers = false;
    protected Graph graph;

    /**
     * Create a State Machine visualisation with the given root node.
     *
     * @param redStates
     */
    public void visualise(List<State> redStates) {
        this.graph = new SingleGraph("State Machine");
        // add style to the graph
        this.graph.addAttribute("ui.antialias");
        this.graph.addAttribute("ui.quality");
        this.graph.addAttribute("ui.stylesheet",
                "node { size: 20px; fill-color: rgb(244, 98, 66); text-size: 12; }" +
                        "node.blue { fill-color: rgb(65, 166, 244); }" +
                        "node.root { size: 25px; fill-color: red; }" +
                        "edge { text-size: 12; }"
        );
        // add all red states
        for (State state : redStates) {
            addNode(state);
        }
        // add all blue states and edges from each red state
        for (State state : redStates) {
            addBlueStates(state);
            addEdges(state);
        }

        if (this.graph.getNodeCount() > 0) {
            // display graph
            this.graph.display();
        }
    }

    /**
     * Add a node for the given state.
     *
     * @param state
     */
    public void addNode(State state) {
        // only add non-empty states
        if (state.count == 0) {
            return;
        }
        String id = Integer.toString(state.hashCode());
        this.graph.addNode(id);
        // give first node, root class
        if (this.graph.getNodeCount() == 1) {
            Node n = this.graph.getNode(id);
            n.addAttribute("ui.class", "root");
        }
        // add label
        this.graph.getNode(id).setAttribute("ui.label", "[" + state.count + "]");
        // give blue states, blue class
        if (state.color == State.Color.BLUE) {
            this.graph.getNode(id).setAttribute("ui.class", "blue");
        }
    }

    /**
     * Add all blue states linked to by the given state.
     *
     * @param state
     */
    public void addBlueStates(State state) {
        for (Symbol symbol : state.getTransitions()) {
            State next = state.getState(symbol);
            if (next.color == State.Color.BLUE) {
                this.addNode(next);
            }
        }
    }

    /**
     * Add an edge between the given states with the given symbol.
     *
     * @param origin
     */
    public void addEdges(State origin) {
        for (Symbol symbol : origin.getTransitions()) {
            String fromId = Integer.toString(origin.hashCode());
            State to = origin.getState(symbol);
            String toId = Integer.toString(to.hashCode());
            String edgeId = fromId + "-" + toId;
            this.graph.addEdge(edgeId, fromId, toId, true);
            String label = symbol.toString();
            // if use_numbers is enabled, use the index of the Symbol instead of the symbol itself
            if (this.use_numbers) {
                for (int i = 0; i < Symbol.values().length; i++) {
                    if (Symbol.values()[i].equals(symbol)) {
                        label = Integer.toString(i);
                        break;
                    }
                }
            }
            // add the label to the edge
            this.graph.getEdge(edgeId).setAttribute("ui.label", label);
        }
    }

}
