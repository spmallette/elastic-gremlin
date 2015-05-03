package com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.flatmap;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.BoolFilterBuilder;

import java.util.*;

public class ElasticEdgeVertexStep extends ElasticFlatMapStep<Edge,Vertex> {

    private final String[] typeLabels;
    private Object[] onlyAllowedIds;
    public ElasticEdgeVertexStep(EdgeVertexStep originalStep, BoolFilterBuilder boolFilter, String[] typeLabels,Object[] onlyAllowedIds, ElasticService elasticService,Integer resultsLimit) {
        super(originalStep.getTraversal(), originalStep.getLabel(), elasticService, boolFilter, originalStep.getDirection(),resultsLimit);
        this.typeLabels = typeLabels;
        this.onlyAllowedIds = onlyAllowedIds;
    }

    @Override
    protected void load(List<ElasticTraverser> traversers) {
        Map<String,List<ElasticTraverser>> vertexIdToTraverser =  new HashMap<>();
        traversers.forEach(traver -> ((ElasticEdge) traver.getElement()).getVertexId(direction).forEach(id -> {
            List<ElasticTraverser> traverserList = vertexIdToTraverser.get(id);
            if (traverserList == null) {
                traverserList = new ArrayList<>();
                vertexIdToTraverser.put(id.toString(), traverserList);
            }
            traverserList.add(traver);
        }));

        Object[] allVertexIds = onlyAllowedIds.length > 0? onlyAllowedIds : vertexIdToTraverser.keySet().toArray();
        Iterator<Vertex> vertexIterator = elasticService.searchVertices(boolFilter, allVertexIds, typeLabels,resultsLimit);

        vertexIterator.forEachRemaining(vertex ->
            vertexIdToTraverser.get(vertex.id()).forEach(traverser ->
                    traverser.addResult(vertex)));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction);
    }
}
