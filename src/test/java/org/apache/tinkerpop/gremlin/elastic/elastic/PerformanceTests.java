package org.apache.tinkerpop.gremlin.elastic.elastic;


import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.elastic.ElasticGraphGraphProvider;
import org.apache.tinkerpop.gremlin.elastic.elasticservice.*;
import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

public class PerformanceTests {

    TimingAccessor sw = new TimingAccessor();


    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    public void testToPassTests() throws IOException, NoSuchMethodException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "testgraph");
        String indexName = "graphtest14";
        config.addProperty("elasticsearch.index.name", indexName.toLowerCase());
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE);
        ElasticGraph graph = new ElasticGraph(config);
        graph.elasticService.clearAllData();
        ElasticGraphGraphProvider elasticGraphProvider = new ElasticGraphGraphProvider();
        Method m = this.getClass().getMethod("testToPassTests");
        LoadGraphWith[] loadGraphWiths = m.getAnnotationsByType(LoadGraphWith.class);
        elasticGraphProvider.loadGraphData(graph, loadGraphWiths[0], this.getClass(), m.getName());

        startWatch("graph repeat");
//        GraphTraversal<Vertex, Map<String, Object>> iter = graph.V("1").as("a").out("knows").as("b").select();
        GraphTraversalSource g = elasticGraphProvider.traversal(graph);
        //GraphTraversal<Vertex, Vertex> iter = g.V("1").outE().has("weight", Compare.inside, Arrays.asList(0.0d, 0.6d)).inV();


        GraphTraversal<Vertex, Long> iter = g.V().repeat(out()).times(2).count();
        //GraphTraversal<Vertex, Map<String, Object>> iter = g.V().has("age").as("a").out().in().has("age").as("b").select();
        System.out.println("iter = " + iter);
        while(iter.hasNext()){
            Object next = iter.next();
            String s = next.toString();
            System.out.println("s = " + s);
        }
        stopWatch("graph repeat");
        sw.print();


        int i =1 ;


        graph.elasticService.client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        graph.close();
    }

    @Test
    public void profile() throws IOException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "test");
        config.addProperty("elasticsearch.index.name", "graph");
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE);

        startWatch("graph initalization");
        ElasticGraph graph = new ElasticGraph(config);
        stopWatch("graph initalization");
        graph.elasticService.clearAllData();

        startWatch("add vertices");
        int count = 10000;
        for(int i = 0; i < count; i++)
            graph.addVertex();
        stopWatch("add vertices");

        startWatch("vertex iterator");
        Iterator<Vertex> vertexIterator = graph.vertices();
        stopWatch("vertex iterator");

        startWatch("add edges");
        vertexIterator.forEachRemaining(v -> v.addEdge("bla", v));
        stopWatch("add edges");

        startWatch("edge iterator");
        Iterator<Edge> edgeIterator = graph.edges();
        stopWatch("edge iterator");

        sw.print();
        System.out.println("-----");
        graph.close();
    }

    @Test
    public void batchLoad() throws IOException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "test");
        config.addProperty("elasticsearch.index.name", "graph");
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.batch", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE);

        startWatch("graph initalization");
        ElasticGraph graph = new ElasticGraph(config);
        stopWatch("graph initalization");
        graph.elasticService.clearAllData();

        startWatch("add vertices");
        int count = 10000;
        for(int i = 0; i < count; i++)
            graph.addVertex();
        graph.commit();
        stopWatch("add vertices");


        startWatch("vertex iterator");
        Iterator<Vertex> vertexIterator = graph.vertices();
        stopWatch("vertex iterator");

        startWatch("add edges");
        vertexIterator.forEachRemaining(v -> v.addEdge("bla", v));
        graph.commit();
        stopWatch("add edges");

        startWatch("edge iterator");
        Iterator<Edge> edgeIterator = graph.edges();
        stopWatch("edge iterator");

        sw.print();
        System.out.println("-----");
        graph.close();
    }

    private void stopWatch(String s) {
        sw.timer(s).stop();
    }

    private void startWatch(String s) {
        sw.timer(s).start();
    }
}
