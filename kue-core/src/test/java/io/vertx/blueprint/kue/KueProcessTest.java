package io.vertx.blueprint.kue;

import io.vertx.blueprint.kue.queue.JobState;
import io.vertx.blueprint.kue.queue.KueVerticle;
import io.vertx.blueprint.kue.queue.Priority;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class KueProcessTest {

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
                        async.complete();
                    } else {
                        context.fail(r2.cause());
                    }
                });
            } else {
                context.fail(r.cause());
            }
        });
    }

    @Test(timeout = 7500)
    public void testProcessDelayedCreateJob(TestContext context) {
        Async async = context.async();
        kue.on("error", error -> {
            context.fail(((JsonObject) error.body()).getString("message"));
        });
        kue.process(TYPE, job -> {
            context.assertEquals(Priority.NORMAL, job.getPriority());
            context.assertEquals(JobState.ACTIVE, job.getState());
            context.assertEquals(new JsonObject().put("data", TYPE + ":data"), job.getData());

            kue.getJob(job.getId()).setHandler(it -> {
                if (it.succeeded()) {
                    context.assertTrue(job.getPromote_at() - job.getCreated_at() >= 3000);
                    async.complete();
                } else {
                    context.fail(it.cause());
                }
            });
        });
        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data")).setDelay(3000).save();
    }

    @Test(timeout = 2500)
    public void testProcessCreateJob(TestContext context) {
        Async async = context.async();
        kue.on("error", error -> {
            context.fail(((JsonObject) error.body()).getString("message"));
        });
        kue.process(TYPE, job -> {
            context.assertEquals(Priority.NORMAL, job.getPriority());
            context.assertEquals(JobState.ACTIVE, job.getState());
            context.assertEquals(new JsonObject().put("data", TYPE + ":data"), job.getData());
            async.complete();
        });
        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data")).save();
    }

//    @Test(timeout = 5000)
//    public void testProcessBlockingCreateJob(TestContext context) {
//        Async async = context.async();
//        kue.on("error", error -> {
//            context.fail(((JsonObject) error.body()).getString("message"));
//        });
//        kue.processBlocking(TYPE, 100, job -> {
//            context.assertEquals(Priority.NORMAL, job.getPriority());
//            context.assertEquals(JobState.ACTIVE, job.getState());
//            context.assertEquals(new JsonObject().put("data", TYPE + ":data"), job.getData());
//            async.complete();
//        });
//        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data")).save();
//    }
}
