package com.tinkerpop.gremlin.elastic.process.graph.traversal.steps;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_P_PA_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.BoolFilterBuilder;

import java.util.*;

public class ElasticGraphStep<E extends Element> extends GraphStep<E> {

    private final BoolFilterBuilder boolFilter;
    private final String[] typeLabels;
    private final ElasticService elasticService;
    private Object[] onlyAllowedIds;
    private Integer resultLimit;
    public ElasticGraphStep(GraphStep originalStep, BoolFilterBuilder boolFilter, String[] typeLabels,Object[] onlyAllowedIds, ElasticService elasticService,Integer resultLimit) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.getIds());
        if (originalStep.getLabel().isPresent()) this.setLabel(originalStep.getLabel().get().toString());
        this.boolFilter = boolFilter;
        this.typeLabels = typeLabels;
        this.elasticService = elasticService;
        this.onlyAllowedIds = onlyAllowedIds;
        this.resultLimit = resultLimit;
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }


    private Iterator<? extends Vertex> vertices() {
        return elasticService.searchVertices(boolFilter, this.getIds(), typeLabels,resultLimit);
    }

    private Iterator<? extends Edge> edges() {
         return elasticService.searchEdges(boolFilter, ids, typeLabels,resultLimit);
    }

    @Override
    public Object[] getIds(){
        return onlyAllowedIds.length > 0? onlyAllowedIds : super.getIds();
    }

    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (PROFILING_ENABLED) TraversalMetrics.start(this);
        try {
            this.start = this.iteratorSupplier.get();
            if (null != this.start) {

                if (this.start instanceof Iterator) {
                    List<E> newListForIterator = new ArrayList<>();
                    Iterator<E> iter = (Iterator<E>) this.start;
                    while(iter.hasNext()){
                        E next = iter.next();

                        //B_O_PA_S_SE_SL_NC_Traverser<E> eb_o_pa_s_se_sl_nc_traverser = new B_O_PA_S_SE_SL_NC_Traverser<>(next, this);
                        this.starts.add(B_O_P_PA_S_SE_SL_TraverserGenerator.instance().generate(next,this,1l));
                        newListForIterator.add(next);
                    }
                    this.start = newListForIterator.iterator();
                    //this.starts.add(traverserGenerator.generateIterator((Iterator<E>) this.start, this, 1l));
                } else {
                    this.starts.add(traverserGenerator.generate((E) this.start, this, 1l));
                }
            }
        }catch (NoSuchElementException ex){
            throw ex;
        }
        catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            if (PROFILING_ENABLED) TraversalMetrics.stop(this);
        }
    }
}
