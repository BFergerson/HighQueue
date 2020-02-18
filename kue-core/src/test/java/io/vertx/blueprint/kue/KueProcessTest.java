package io.vertx.blueprint.kue;

import io.vertx.blueprint.kue.queue.JobState;
import io.vertx.blueprint.kue.queue.KueVerticle;
import io.vertx.blueprint.kue.queue.Priority;
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
public class KueProcessTest {

    private static final Logger logger = LoggerFactory.getLogger(KueProcessTest.class);

    private static final int PORT = 8080;
    private static final String HOST = "localhost";

    private static final String TYPE = "test:inserts";
    private static final String TYPE_DELAYED = "test_delayed:inserts";

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

    @Test(timeout = 7500)
    public void testProcessDelayedCreateJob(TestContext context) {
        Async async = context.async();
        kue.process(TYPE_DELAYED, job -> {
            context.assertEquals(Priority.NORMAL, job.getPriority());
            context.assertEquals(JobState.ACTIVE, job.getState());
            context.assertEquals(new JsonObject().put("data", TYPE_DELAYED + ":data"), job.getData());

            kue.getJob(job.getId()).setHandler(it -> {
                if (it.succeeded()) {
                    context.assertTrue(job.getPromote_at() - job.getCreated_at() >= 3000);
                    async.complete();
                } else {
                    context.fail(it.cause());
                }
            });
        });
        kue.createJob(TYPE_DELAYED, new JsonObject().put("data", TYPE_DELAYED + ":data"))
                .setDelay(3000)
                .save().setHandler(it -> {
            if (it.failed()) {
                context.fail(it.cause());
            }
        });
    }

    @Test(timeout = 2500)
    public void testProcessCreateJob(TestContext context) {
        Async async = context.async();
        kue.process(TYPE, job -> {
            context.assertEquals(Priority.NORMAL, job.getPriority());
            context.assertEquals(JobState.ACTIVE, job.getState());
            context.assertEquals(new JsonObject().put("data", TYPE + ":data"), job.getData());
            async.complete();
        });
        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
                .save().setHandler(it -> {
            if (it.failed()) {
                context.fail(it.cause());
            }
        });
    }

    @Test(timeout = 2500)
    public void testProcessTwoJobs(TestContext context) {
        Async async = context.async(2);
        kue.process(TYPE, job -> {
            Async getJobsAsync = context.async();
            if (job.getId() == 1) {
                //verify second job inactive
                kue.getJob(2).setHandler(job2 -> {
                    if (job2.succeeded()) {
                        context.assertTrue(job2.result().isPresent());
                        context.assertEquals(JobState.INACTIVE, job2.result().get().getState());
                        getJobsAsync.countDown();
                    } else {
                        context.fail(job2.cause());
                    }
                });
            } else if (job.getId() == 2) {
                //verify first job complete
                kue.getJob(1).setHandler(job1 -> {
                    if (job1.succeeded()) {
                        context.assertTrue(job1.result().isPresent());
                        context.assertEquals(JobState.COMPLETE, job1.result().get().getState());
                        getJobsAsync.countDown();
                    } else {
                        context.fail(job1.cause());
                    }
                });
            } else {
                context.fail("Invalid job id: " + job.getId());
            }

            getJobsAsync.handler(it -> {
                if (it.succeeded()) {
                    async.countDown();
                } else {
                    context.fail(it.cause());
                }
            });
            context.assertEquals(Priority.NORMAL, job.getPriority());
            context.assertEquals(JobState.ACTIVE, job.getState());
            context.assertEquals(new JsonObject().put("data", TYPE + ":data"), job.getData());
            job.done();
        });
        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
                .save().setHandler(it -> {
            if (it.failed()) {
                context.fail(it.cause());
            } else {
                kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
                        .save().setHandler(it2 -> {
                    if (it2.failed()) {
                        context.fail(it2.cause());
                    }
                });
            }
        });
    }

//    @Test(timeout = 2500)
//    public void testProcessTwoJobsSimultaneously(TestContext context) {
//        Async async = context.async(2);
//        kue.process(TYPE, 2, job -> {
//            //verify both jobs active/completed
//            Async getJobsAsync = context.async(2);
//            kue.getJob(1).setHandler(job1 -> {
//                if (job1.succeeded()) {
//                    context.assertTrue(job1.result().isPresent());
//                    context.assertTrue(JobState.ACTIVE == job1.result().get().getState()
//                            || JobState.COMPLETE == job1.result().get().getState());
//                    getJobsAsync.countDown();
//                } else {
//                    context.fail(job1.cause());
//                }
//            });
//            kue.getJob(2).setHandler(job2 -> {
//                if (job2.succeeded()) {
//                    context.assertTrue(job2.result().isPresent());
//                    System.out.println("Job2 state: " + job2.result().get().getState());
//                    context.assertTrue(JobState.ACTIVE == job2.result().get().getState()
//                            || JobState.COMPLETE == job2.result().get().getState());
//                    getJobsAsync.countDown();
//                } else {
//                    context.fail(job2.cause());
//                }
//            });
//
//            getJobsAsync.handler(it -> {
//                if (it.succeeded()) {
//                    async.countDown();
//                } else {
//                    context.fail(it.cause());
//                }
//            });
//            context.assertEquals(Priority.NORMAL, job.getPriority());
//            context.assertEquals(JobState.ACTIVE, job.getState());
//            context.assertEquals(new JsonObject().put("data", TYPE + ":data"), job.getData());
//            job.done();
//        });
//        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
//                .save().setHandler(it -> {
//            if (it.failed()) {
//                context.fail(it.cause());
//            } else {
//                kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
//                        .save().setHandler(it2 -> {
//                    if (it2.failed()) {
//                        context.fail(it2.cause());
//                    }
//                });
//            }
//        });
//    }

//    @Test(timeout = 7500)
//    public void testProcessBlockingCreateJob(TestContext context) {
//        Async async = context.async(2);
//        kue.processBlocking(TYPE, 10, job -> {
//            context.assertEquals(Priority.NORMAL, job.getPriority());
//            context.assertEquals(JobState.ACTIVE, job.getState());
//            context.assertEquals(new JsonObject().put("data", TYPE + ":data"), job.getData());
//            try {
//                Thread.sleep(5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            async.countDown();
//        });
//        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
//                .save().setHandler(it -> {
//            if (it.failed()) {
//                context.fail(it.cause());
//            }
//        });
//        kue.createJob(TYPE, new JsonObject().put("data", TYPE + ":data"))
//                .save().setHandler(it -> {
//            if (it.failed()) {
//                context.fail(it.cause());
//            }
//        });
//    }
}
