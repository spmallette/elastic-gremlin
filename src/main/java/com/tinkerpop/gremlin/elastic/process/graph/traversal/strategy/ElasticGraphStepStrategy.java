package com.tinkerpop.gremlin.elastic.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.*;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.flatmap.*;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.traversalHolder.ElasticRepeatStep;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.traversalHolder.ElasticUnionStep;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.Contains;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;

import java.util.*;

public class ElasticGraphStepStrategy extends AbstractTraversalStrategy {
    private static final ElasticGraphStepStrategy INSTANCE = new ElasticGraphStepStrategy();

    public static ElasticGraphStepStrategy instance() {
        return INSTANCE;
    }
    private static ThreadLocal<ElasticGraph> graph = new ThreadLocal<>();


    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isComputer()) return;

        Step<?, ?> startStep = traversal.getStartStep();
        if(startStep instanceof GraphStep || graph != null) {
            if(startStep instanceof GraphStep ) graph.set((ElasticGraph)traversal.getGraph().get());
            processStep(startStep, traversal, graph.get().elasticService);
        }
    }

    private void processStep(Step<?, ?> currentStep, Traversal.Admin<?, ?> traversal, ElasticService elasticService) {
        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
        List<String> typeLabels = new ArrayList<>();
        List<Object> onlyIdsAllowed = new ArrayList<>();
        Step<?, ?> nextStep = currentStep.getNextStep();

        while(stepCanConsumePredicates(currentStep) && nextStep instanceof HasContainerHolder) {
            final boolean[] containesLambada = {false};
            ((HasContainerHolder) nextStep).getHasContainers().forEach((has)->{
                if(has.predicate.toString().equals("eq") && has.key.equals("~label"))
                    typeLabels.add(has.value.toString());
                else if(has.predicate.toString().equals("eq") && has.key.equals("~id"))
                    onlyIdsAllowed.add(has.value);
                else if(has.predicate.toString().contains("$$")){
                    containesLambada[0] = true;
                }
                else addFilter(boolFilter, has);
            });
            //if the step containes as("label")
            if (nextStep.getLabel().isPresent()) {
                final IdentityStep identityStep = new IdentityStep<>(traversal);
                identityStep.setLabel(nextStep.getLabel().get());
                TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
            }
            if(!containesLambada[0]) traversal.removeStep(nextStep);
            nextStep  = nextStep.getNextStep();
        }

        Integer resultsLimit = null;
        if(nextStep instanceof RangeStep){
           RangeStep rangeStep = (RangeStep) nextStep;
            if (isLimitStep(rangeStep) && stepCanConsumePredicates(currentStep)){
                resultsLimit = (int) rangeStep.getHighRange();
                traversal.removeStep(nextStep);
            }
        }
        String[] typeLabelsArray = typeLabels.toArray(new String[0]);
        Object[] onlyIdsAllowedArray = onlyIdsAllowed.toArray(new Object[onlyIdsAllowed.size()]);

        if (currentStep instanceof GraphStep) {
            final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>((GraphStep) currentStep, boolFilter, typeLabelsArray,onlyIdsAllowedArray, elasticService,resultsLimit);
            TraversalHelper.replaceStep(currentStep, (Step) elasticGraphStep, traversal);
        }
        else if (currentStep instanceof VertexStep) {
            ElasticVertexStep<Element> elasticVertexStep = new ElasticVertexStep<>((VertexStep) currentStep, boolFilter, typeLabelsArray,onlyIdsAllowedArray, elasticService,resultsLimit);
            TraversalHelper.replaceStep(currentStep, (Step) elasticVertexStep, traversal);
        }
        else if (currentStep instanceof EdgeVertexStep){
            ElasticEdgeVertexStep newSearchStep = new ElasticEdgeVertexStep((EdgeVertexStep)currentStep, boolFilter, typeLabelsArray,onlyIdsAllowedArray, elasticService,resultsLimit);
            TraversalHelper.replaceStep(currentStep, (Step) newSearchStep, traversal);
        }
        else if (currentStep instanceof RepeatStep){
            RepeatStep originalRepeatStep = (RepeatStep) currentStep;
            ElasticRepeatStep repeatStep = new ElasticRepeatStep(currentStep.getTraversal(),originalRepeatStep);
            TraversalHelper.replaceStep(currentStep, (Step) repeatStep, traversal);
        }
        else if (currentStep instanceof LocalStep){
            //local step is working on each vertex -> we don't want our strategy to apply on this step
            LocalStep localStep = (LocalStep) currentStep;
            ((Traversal) localStep.getTraversals().get(0)).asAdmin().setStrategies(TraversalStrategies.GlobalCache.getStrategies(Graph.class));
        }
        else if (currentStep instanceof UnionStep){
            UnionStep originalUnionStep = (UnionStep) currentStep;
            ElasticUnionStep unionStep = new ElasticUnionStep(currentStep.getTraversal(),originalUnionStep);
            TraversalHelper.replaceStep(currentStep, (Step) unionStep, traversal);
        }
        else {
            //TODO
        }

        if(!(currentStep instanceof EmptyStep)) processStep(nextStep, traversal, elasticService);
    }



    private boolean stepCanConsumePredicates(Step<?, ?> step) {
        return (step instanceof VertexStep)|| (step instanceof EdgeVertexStep ) || (step instanceof GraphStep);
    }

    private void addFilter(BoolFilterBuilder boolFilterBuilder, HasContainer has){
        if (has.predicate instanceof Compare) {
            String predicateString = has.predicate.toString();
            switch (predicateString) {
                case ("eq"):
                    boolFilterBuilder.must(FilterBuilders.termFilter(has.key, has.value));
                    break;
                case ("neq"):
                    boolFilterBuilder.mustNot(FilterBuilders.termFilter(has.key, has.value));
                    break;
                case ("gt"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gt(has.value));
                    break;
                case ("gte"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gte(has.value));
                    break;
                case ("lt"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lt(has.value));
                    break;
                case ("lte"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lte(has.value));
                    break;
                default:
                    throw new IllegalArgumentException("predicate not supported in has step: " + has.predicate.toString());
            }
        } else if (has.predicate instanceof Contains) {
            if (has.predicate == Contains.without) boolFilterBuilder.must(FilterBuilders.missingFilter(has.key));
            else if (has.predicate == Contains.within){
                if(has.value == null) boolFilterBuilder.must(FilterBuilders.existsFilter(has.key));
                else  boolFilterBuilder.must(FilterBuilders.termsFilter(has.key, has.value));
            }
        } else if (has.predicate instanceof Geo) boolFilterBuilder.must(new GeoShapeFilterBuilder(has.key, GetShapeBuilder(has.value), ((Geo) has.predicate).getRelation()));
        else throw new IllegalArgumentException("predicate not supported by elastic-gremlin: " + has.predicate.toString());
    }

    private ShapeBuilder GetShapeBuilder(Object object) {
        try {
            String geoJson = (String) object;
            XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
            parser.nextToken();

            return ShapeBuilder.parse(parser);
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isLimitStep(RangeStep rangeStep) {
        return rangeStep.getLowRange() == 0;
    }


}
