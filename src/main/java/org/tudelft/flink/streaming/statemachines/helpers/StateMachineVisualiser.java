package org.tudelft.flink.streaming.statemachines.helpers;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSinkImages;
import org.tudelft.flink.streaming.statemachines.State;
import org.tudelft.flink.streaming.statemachines.Symbol;

import java.io.IOException;
import java.util.*;

public class StateMachineVisualiser {

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
    }

    /**
     * Show the State Machine visualisation.
     */
    public void showVisualisation() {
        if (this.graph.getNodeCount() > 0) {
            this.graph.display();
        }
    }

    /**
     * Write the visualisation to file.
     *
     * @param stateMachineID
     */
    public void writeToFile(String stateMachineID) {
        // don't write empty graphs
        if (this.graph.getNodeCount() == 0) {
            return;
        }
        // create image file sink
        FileSinkImages sink = new FileSinkImages(FileSinkImages.OutputType.PNG, FileSinkImages.Resolutions.HD1080);
        sink.setLayoutPolicy(FileSinkImages.LayoutPolicy.COMPUTED_FULLY_AT_NEW_IMAGE);
        // get file path
        String cleanID = stateMachineID.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
        String path = "output\\state-machines\\machine-" + cleanID + ".png";
        // write graph to sink
        try {
            sink.writeAll(this.graph, path);
        } catch (IOException ex) {
            System.out.println("Error while saving graph to file:\n" + ex.getMessage());
        }
    }

    /**
     * Add a node for the given state.
     *
     * @param state
     */
    public void addNode(State state) {
        // only add non-empty states
        if (state.getCount() == 0) {
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
        this.graph.getNode(id).setAttribute("ui.label", "[" + state.getCount() + "]");
        // give blue states, blue class
        if (state.getColor() == State.Color.BLUE) {
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
            if (next.getColor() == State.Color.BLUE) {
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
            // add the label to the edge
            this.graph.getEdge(edgeId).setAttribute("ui.label", label);
        }
    }

}
