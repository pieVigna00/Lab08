package it.polito.tdp.extflightdelays.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import it.polito.tdp.extflightdelays.db.ExtFlightDelaysDAO;

public class Model {
	private Graph<Airport, DefaultWeightedEdge> graph;
	private ExtFlightDelaysDAO dao;
	private Map<Integer, Airport> idMapAereoporto;
	private List<Rotta> edges;
	public Model() {
		this.graph=new SimpleWeightedGraph<>(DefaultWeightedEdge.class);
	    this.dao= new ExtFlightDelaysDAO();
	    this.idMapAereoporto=new HashMap<>();
		this.dao.loadAllAirports(idMapAereoporto);
		this.edges=new ArrayList<Rotta>();
	}
	public void buildGraph(int distanza) {
		this.graph=new SimpleWeightedGraph<>(DefaultWeightedEdge.class);
		Graphs.addAllVertices(this.graph, this.dao.getVertici(distanza, idMapAereoporto));
		edges= this.dao.getRotte(distanza, idMapAereoporto);
		for(Rotta e: edges) {
			Airport origin = e.getA1();
			Airport destination=e.getA2();
			int peso=e.getDistanza();
		
		if(graph.vertexSet().contains(origin) && graph.vertexSet().contains(destination) ) {
			DefaultWeightedEdge edge=this.graph.getEdge(origin, destination);
			if(edge!=null) {
				double weight=this.graph.getEdgeWeight(edge); //per esempio abbiamo già messo da a a b e noi stiamo mettendo da b ad a
				weight +=peso;//aggiungo peso rotta opposta 
				this.graph.setEdgeWeight(origin,  destination, weight);
			}else {
				this.graph.addEdge(origin, destination);
				this.graph.setEdgeWeight(origin,  destination, peso);
				//aggrego archi verso apposto, faccio qua al posto che fare query 
			}
			}
		}
	}
	public int getNumeroVertici(){
		return this.graph.vertexSet().size();
	}
	public int getNumeroArchi() {
		return this.graph.edgeSet().size();
	}
	public String getArchi(){
		String result="";
		for(Rotta r: edges) {
			 result+="rotta: " +r+"con distanza media: "+r.getDistanza()+"\n";
		}
		return result;
	}
}
