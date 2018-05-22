package org.tudelft.flink.streaming.statemachines.visualisation;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.stream.file.FileSinkImages;

import java.io.IOException;

public class StateMachineVisualiser {

    protected Graph graph;

    public StateMachineVisualiser() {
        // set advanced UI renderer
        System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");

        this.graph = new MultiGraph("State Machine");
        // add style to the graph
        this.graph.addAttribute("ui.antialias");
        this.graph.addAttribute("ui.quality");
        this.graph.addAttribute("ui.stylesheet",
                "node { size: 28; fill-color: rgb(244, 98, 66); text-size: 16; text-color: #FFF; }" +
                        "node.blue { fill-color: rgb(65, 166, 244); }" +
                        "node.root { fill-color: orange; }" +
                        "edge { text-size: 16; }"
        );
    }

    /**
     * Show the State Machine visualisation.
     */
    public void showVisualisation() {
        if (this.graph.getNodeCount() > 1) {
            this.graph.display();
        }
    }

    /**
     * Write the visualisation to file.
     *
     * @param path
     */
    protected void writeToFile(String path) {
        // don't write empty graphs
        if (this.graph.getNodeCount() <= 1) {
            return;
        }
        // create image file sink
        FileSinkImages sink = new FileSinkImages(FileSinkImages.OutputType.PNG, FileSinkImages.Resolutions.HD1080);
        sink.setLayoutPolicy(FileSinkImages.LayoutPolicy.COMPUTED_FULLY_AT_NEW_IMAGE);
        sink.setRenderer(FileSinkImages.RendererType.SCALA);
        // write graph to sink
        try {
            sink.writeAll(this.graph, path);
            sink.end();
        } catch (IOException ex) {
            System.out.println("Error while saving graph to file:\n" + ex.getMessage());
        }
    }

    /**
     * Add a node to the graph for the given state.
     *
     * @param id
     * @param cssClass
     * @param label
     */
    protected void addState(String id, String cssClass, String label) {
        // prevent adding states multiple times
        if (this.graph.getNode(id) != null) {
            return;
        }
        // add node to graph
        this.graph.addNode(id);
        // add the state label
        this.graph.getNode(id).setAttribute("ui.label", label);
        // give states their css class
        if (cssClass != null) {
            this.graph.getNode(id).setAttribute("ui.class", cssClass);
        }
        // give first node, root class
        if (this.graph.getNodeCount() == 1) {
            Node n = this.graph.getNode(id);
            n.addAttribute("ui.class", "root");
        }
    }

    /**
     * Add a new transition to the graph.
     *
     * @param from
     * @param to
     * @param label         the symbol or other label that will be shown on the edge
     */
    protected void addTransition(String from, String to, String label) {
        // generate unique id for this edge
        String edgeId = from + "-" + to;
        // add edge
        this.graph.addEdge(edgeId, from, to, true);
        // add the label to the edge
        this.graph.getEdge(edgeId).setAttribute("ui.label", label);
    }

}
