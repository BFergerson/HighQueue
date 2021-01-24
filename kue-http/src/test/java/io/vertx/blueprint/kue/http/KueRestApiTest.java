package io.vertx.blueprint.kue.http;

import io.vertx.blueprint.kue.Kue;
import io.vertx.blueprint.kue.queue.Job;
import io.vertx.blueprint.kue.queue.KueVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

/**
 * Vert.x Kue REST API test case
 *
 * @author Eric Zhao
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(VertxUnitRunner.class)
public class KueRestApiTest {

    private static final int PORT = 8080;
    private static final String HOST = "localhost";

    private static final String TYPE = "test:inserts";

    private static Kue kue;

    @BeforeClass
    public static void setUp(TestContext context) throws Exception {
        Async async = context.async();
        Vertx.clusteredVertx(new VertxOptions(), r -> {
            if (r.succeeded()) {
                Vertx vertx = r.result();
                kue = Kue.createQueue(vertx, new JsonObject());
                vertx.deployVerticle(new KueVerticle(kue), r2 -> {
                    if (r2.succeeded()) {
                        kue.jobRangeByType(TYPE, "inactive", 0, 100, "asc").onComplete(r1 -> {
                            if (r1.succeeded()) {
                                r1.result().forEach(Job::remove);
                                vertx.deployVerticle(new KueHttpVerticle(), r3 -> {
                                    if (r3.succeeded())
                                        async.complete();
                                    else
                                        context.fail(r3.cause());
                                });
                            } else {
                                context.fail(r1.cause());
                            }
                        });
                    } else {
                        context.fail(r2.cause());
                    }
                });

            } else {
                context.fail(r.cause());
            }
        });
    }

    @Test
    public void testApiStats(TestContext context) throws Exception {
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        Async async = context.async();
        client.request(HttpMethod.GET, PORT, HOST, "/stats", it -> {
            context.assertTrue(it.succeeded());
            it.result().send(rsp -> {
                context.assertTrue(rsp.succeeded());
                rsp.result().bodyHandler(body -> {
                    JsonObject stats = new JsonObject(body.toString());
                    context.assertEquals(stats.getInteger("inactiveCount") > 0, true);
                    client.close();
                    async.complete();
                });
            });
        });
    }

    public void testApiTypeStateStats(TestContext context) throws Exception {
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        Async async = context.async();
    }

    public void testJobTypes(TestContext context) throws Exception {
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        Async async = context.async();
    }

    public void testJobRange(TestContext context) throws Exception {
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        Async async = context.async();
    }

    public void testJobTypeRange(TestContext context) throws Exception {
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        Async async = context.async();
    }

    public void testJobStateRange(TestContext context) throws Exception {
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        Async async = context.async();
    }

    @Test
    public void testApiGetJob(TestContext context) throws Exception {
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        Async async = context.async();
        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
                .save()
                .onComplete(jr -> {
                    if (jr.succeeded()) {
                        long id = jr.result().getId();
                        client.request(HttpMethod.GET, PORT, HOST, "/job/" + id, it -> {
                            context.assertTrue(it.succeeded());
                            it.result().send(rsp -> {
                                context.assertTrue(rsp.succeeded());
                                rsp.result().bodyHandler(body -> {
                                    context.assertEquals(new Job(new JsonObject(body.toString())).getId(), id);
                                    client.close();
                                    async.complete();
                                });
                            });
                        });
                    } else {
                        context.fail(jr.cause());
                    }
                });
    }

    @Test
    public void testDeleteJob(TestContext context) throws Exception {
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        Async async = context.async();
        client.request(HttpMethod.DELETE, PORT, HOST, "/job/66", it -> {
            context.assertTrue(it.succeeded());
            it.result().send(rsp -> {
                context.assertTrue(rsp.succeeded());
                context.assertEquals(204, rsp.result().statusCode());
                client.close();
                async.complete();
            });
        });
    }

    @Test
    public void testApiCreateJob(TestContext context) throws Exception {
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        Async async = context.async();
        Job job = kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"));
        client.request(HttpMethod.PUT, PORT, HOST, "/job", it -> {
            context.assertTrue(it.succeeded());
            it.result().putHeader("content-type", "application/json").end(job.toString());
            it.result().send(rsp -> {
                context.assertEquals(201, rsp.result().statusCode());
                rsp.result().bodyHandler(body -> {
                    context.assertEquals(new JsonObject(body.toString()).getString("message"), "job created");
                    client.close();
                    async.complete();
                });
            });
        });
    }

}