package com.tinkerpop.gremlin.elastic.structure;


import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

public class ElasticVertexProperty<V> implements VertexProperty<V> {

    private final ElasticVertex vertex;
    private final String key;
    private final V value;

    public ElasticVertexProperty(final ElasticVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public Object id() {
        return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw VertexProperty.Exceptions.multiPropertiesNotSupported();
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... strings) {
        throw VertexProperty.Exceptions.multiPropertiesNotSupported();
    }

    @Override
    public void remove() {
        vertex.removeProperty(this);
    }
}
