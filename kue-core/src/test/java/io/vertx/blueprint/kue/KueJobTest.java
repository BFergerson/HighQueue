package io.vertx.blueprint.kue;

import io.vertx.blueprint.kue.queue.KueVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

@RunWith(VertxUnitRunner.class)
public class KueJobTest {

    private static final Logger logger = LoggerFactory.getLogger(KueJobTest.class);

    private static final String TYPE = "test:inserts";

    private static Kue kue;
    private Vertx vertx;

    @Before
    public void setUp(TestContext context) {
        Async async = context.async();
        vertx = Vertx.vertx();
        kue = Kue.createQueue(vertx, new JsonObject());
        vertx.deployVerticle(new KueVerticle(kue), r2 -> {
            if (r2.succeeded()) {
                logger.info("Clearing Redis");
                kue.getRedisAPI().flushall(Collections.emptyList(), it -> {
                    if (it.succeeded()) {
                        async.complete();
                    } else {
                        context.fail(it.cause());
                    }
                });
            } else {
                context.fail(r2.cause());
            }
        });
    }

    @After
    public void tearDown(TestContext context) {
        logger.info("Tearing down Vertx");
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testGetAllTypes(TestContext context) {
        Async async = context.async();
        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
                .save().onComplete(it -> {
            if (it.succeeded()) {
                kue.getAllTypes().onComplete(it2 -> {
                    if (it2.succeeded()) {
                        context.assertEquals(1, it2.result().size());
                        context.assertEquals(TYPE, it2.result().get(0));
                        async.complete();
                    } else {
                        context.fail(it2.cause());
                    }
                });
            } else {
                context.fail(it.cause());
            }
        });
    }

    @Test
    public void testGetJobLog(TestContext context) {
        Async async = context.async();
        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
                .save().onComplete(it -> {
            if (it.succeeded()) {
                it.result().log("Hello world").onComplete(it2 -> {
                    if (it2.succeeded()) {
                        kue.getJobLog(it.result().getId()).onComplete(it3 -> {
                            if (it3.succeeded()) {
                                context.assertEquals(1, it3.result().size());
                                context.assertEquals("Hello world", it3.result().getString(0));
                                async.complete();
                            } else {
                                context.fail(it3.cause());
                            }
                        });
                    } else {
                        context.fail(it2.cause());
                    }
                });
            } else {
                context.fail(it.cause());
            }
        });
    }
}
