package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.elasticgremlin.elasticservice.*;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;

import java.util.*;

public class ElasticVertex extends ElasticElement implements Vertex {
    private LazyGetter lazyGetter;

    public ElasticVertex(final Object id, final String label, Object[] keyValues, ElasticGraph graph, Boolean lazy) {
        super(id, label, graph, keyValues);
        //if(!(this.id() instanceof String)) throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        if(lazy) {
            this.lazyGetter = graph.elasticService.getLazyGetter();
            lazyGetter.register(this);
        }
    }

    @Override
    public String label() {
        if(this.label == null && lazyGetter != null)
            lazyGetter.execute();
        return super.label();
    }

    @Override
    public Property createProperty(String key, Object value) {
        return new ElasticVertexProperty(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... propertyKeys) {
        checkRemoved();
        if(propertyKeys != null && propertyKeys.length > 0) VertexProperty.Exceptions.metaPropertiesNotSupported();

        return this.property(key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... propertyKeys) {
        checkRemoved();
        if(propertyKeys != null && propertyKeys.length > 0) throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        return this.property(key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        Predicates predicates = new Predicates();
        if(edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));

        return graph.elasticService.searchEdges(predicates, direction, this.id());    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        checkRemoved();
        Iterator<Edge> edgeIterator = edges(direction, edgeLabels);
        ArrayList<Vertex> vertices = new ArrayList<>();
        edgeIterator.forEachRemaining(edge -> vertices.add(vertexToVertex(this, (ElasticEdge) edge, direction)));
        return vertices.iterator();
    }

    public static Vertex vertexToVertex(Vertex originalVertex, ElasticEdge edge, Direction direction) {
        switch (direction) {
            case OUT:
                return edge.inVertex();
            case IN:
                return edge.outVertex();
            case BOTH:
                if(edge.outId.equals(edge.inId))
                    return originalVertex; //points to self
                if(originalVertex.id().equals(edge.inId))
                    return edge.outVertex();
                if(originalVertex.id().equals(edge.outId))
                    return edge.inVertex();
            default:
                throw new IllegalArgumentException(direction.toString());
        }
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        checkRemoved();
        ElementHelper.validateProperty(key, value);
        ElasticVertexProperty vertexProperty = (ElasticVertexProperty) addPropertyLocal(key, value);
        try {
            graph.elasticService.addElement(this, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return vertexProperty;
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        checkRemoved();
        if(lazyGetter != null)
            lazyGetter.execute();
        if (this.properties.containsKey(key)) {
            return (VertexProperty<V>) this.properties.get(key);
        }
        else return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        checkRemoved();
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        ElasticEdge elasticEdge = new ElasticEdge(idValue, label, id(), label(), vertex.id(), vertex.label(), keyValues, this.graph);
        try {
            graph.elasticService.addElement(elasticEdge, true);
        }
        catch(DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        }
        return elasticEdge;
    }

    @Override
    public void remove() {
        checkRemoved();

        ArrayList elements  = new ArrayList(){};
        elements.add(this);
        edges(Direction.BOTH).forEachRemaining(edge -> elements.add(edge));

        graph.elasticService.deleteElements(elements.iterator());
        this.removed = true;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        checkRemoved();
        if(lazyGetter != null) lazyGetter.execute();
        return innerPropertyIterator(propertyKeys);
    }


    @Override
    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
