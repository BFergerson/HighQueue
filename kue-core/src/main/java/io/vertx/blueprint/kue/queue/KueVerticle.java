package io.vertx.blueprint.kue.queue;

import io.vertx.blueprint.kue.Kue;
import io.vertx.blueprint.kue.service.JobService;
import io.vertx.blueprint.kue.service.impl.JobServiceImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ServiceBinder;

import java.util.Collections;

/**
 * Vert.x Blueprint - Job Queue
 * Kue Verticle
 *
 * @author Eric Zhao
 */
public class KueVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(Job.class);

    public static final String EB_JOB_SERVICE_ADDRESS = "vertx.kue.service.job.internal";

    private final Kue kue;
    private JsonObject config;
    private JobService jobService;

    public KueVerticle(Kue kue) {
        this.kue = kue;
    }

    @Override
    public void start(Promise<Void> future) throws Exception {
        this.config = config();
        this.jobService = new JobServiceImpl(kue, config, kue.getRedisAPI());
        kue.getClient().connect(it -> {
            if (it.succeeded()) {
                testConnection(future);
            } else {
                future.fail(it.cause());
            }
        });
    }

    @Override
    public void stop() {
        logger.debug("Closing Kue");
        kue.setClosed(true);
        kue.getClient().close();
        logger.info("Closed Kue");
    }

    private void testConnection(Promise<Void> future) {
        kue.getRedisAPI().ping(Collections.emptyList(), pr -> { // test connection
            if (pr.succeeded()) {
                logger.info("Kue Verticle is running...");

                // register job service
                new ServiceBinder(vertx).setAddress(EB_JOB_SERVICE_ADDRESS)
                        .register(JobService.class, jobService);

                future.complete();
            } else {
                logger.error("oops!", pr.cause());
                future.fail(pr.cause());
            }
        });
    }
}
