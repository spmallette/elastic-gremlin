package org.apache.tinkerpop.gremlin.elastic.process.graph.traversal.steps.traversalHolder;


import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Predicate;

public class ElasticRepeatStep<S> extends ComputerAwareStep<S, S> implements TraversalParent {

    private Traversal.Admin<S, S> repeatTraversal;
    private Traversal.Admin<S, ?> untilTraversal;
    private Traversal.Admin<S, ?> emitTraversal;
    private RepeatStep<S> originalStep;

    public ElasticRepeatStep(Traversal.Admin traversal,RepeatStep<S> originalStep)
    {
        super(traversal);
        this.originalStep = originalStep;
        try {
            Field repeatField = originalStep.getClass().getDeclaredField("repeatTraversal");
            repeatField.setAccessible(true);
            this.repeatTraversal = (Traversal.Admin<S, S>) repeatField.get(originalStep);
            Field untilField = originalStep.getClass().getDeclaredField("untilTraversal");
            untilField.setAccessible(true);
            this.untilTraversal = (Traversal.Admin<S, ?>) untilField.get(originalStep);
            Field emitField = originalStep.getClass().getDeclaredField("emitTraversal");
            emitField.setAccessible(true);
            this.emitTraversal = (Traversal.Admin<S, ?>) emitField.get(originalStep);
        }
        catch (Exception e){

        }
    }

    public List<Traversal.Admin<S, S>> getGlobalChildren() {
        return null == this.repeatTraversal ? Collections.emptyList() : Collections.singletonList(this.repeatTraversal);
    }

    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        final List<Traversal.Admin<S, ?>> list = new ArrayList<>();
        if (null != this.untilTraversal)
            list.add(this.untilTraversal);
        if (null != this.emitTraversal)
            list.add(this.emitTraversal);
        return list;
    }



    @Override
    public Set<TraverserRequirement> getRequirements() {
       return originalStep.getRequirements();
    }

    public final boolean doUntil(final Traverser.Admin<S> traverser,boolean untilFirst) {
        return originalStep.doUntil(traverser,untilFirst);
    }

    public final boolean doEmit(final Traverser.Admin<S> traverser,boolean emitFirst) {
        return originalStep.doEmit(traverser,emitFirst);
    }

    @Override
    public String toString() {
        return originalStep.toString();
    }

    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {

        while (true) {

            if (repeatTraversal.getEndStep().hasNext()) {
                return this.repeatTraversal.getEndStep();
            } else {
                do {
                    Traverser.Admin<S> start = this.starts.next();
                    if (doUntil(start, true)) {
                        start.resetLoops();
                        return IteratorUtils.of(start);
                    }
                    this.repeatTraversal.addStart(start);
                    if (doEmit(start, true)) {
                        final Traverser.Admin<S> emitSplit = start.split();
                        emitSplit.resetLoops();
                        return IteratorUtils.of(emitSplit);
                    }
                } while (this.starts.hasNext());
            }
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        return null;
    }

}
