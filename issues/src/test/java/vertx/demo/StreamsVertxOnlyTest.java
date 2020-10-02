package vertx.demo;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.dropwizard.Match;
import io.vertx.ext.dropwizard.MatchType;
import io.vertx.ext.dropwizard.MetricsService;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StreamsVertxOnlyTest {

    String json = "{\"nfInstanceID\": \"57438718-45c0-4000-8000-095413429196\",\"nfType\": \"BSF\",\"nfStatus\": \"REGISTERED\",\"fqdn\": \"bsf.5gc.mnc001.mcc001.3gppnetwork.org\",\"nfSetIdList\": [ \"set1.BSFset.5gc.mnc001.mcc001\" ],\"heartBeatTimer\": 30,\"priority\": 0,\"capacity\": 10,\"load\": 0,\"locality\": \"NetNumber\",\"nfProfileChangesSupportInd\": false,\"bsfInfo\": {},\"nfServices\": [{\"serviceInstanceID\": \"nbsf-management\",\"serviceName\": \"nbsf-management\",\"versions\": [{\"apiVersionInUri\": \"v1\",\"apiFullVersion\": \"1.1.0\"}],\"scheme\": \"http\",\"nfServiceStatus\": \"REGISTERED\",\"ipEndPoints\": [{\"ipv4Address\": \"127.0.0.1\",\"transport\": \"TCP\",\"port\": 3026},{\"ipv4Address\": \"127.0.0.1\",\"transport\": \"TCP\",\"port\": 3028}]}]}";

    @DataProvider
    public Object[][] tests() {
        return new Object[][]{
            new Object[] //
            {10, 10, -1, 100, 10},
            {250, 8, -1, 100, 1}, //this fails, expecting 1 when it returns 8 connections
            {10, 20, 5, 100, 20}
        //
        };

    }

    @Test(dataProvider = "tests")
    public void testServerMaxStreams(
            int HTTP_SERVER_MAX_CONCURRENT_STREAMS,
            int HTTP_CLIENT_MAX_CONNECTIONS,
            int CLIENT_MULTIPLEX_MAX,
            int TOTAL_REQUESTS,
            int EXPECTED_CONNECTIONS) throws Exception {

        VertxOptions vopts = new VertxOptions().setMetricsOptions(
                new DropwizardMetricsOptions().
                        setEnabled(true).
                        addMonitoredHttpServerUri(new Match().setValue("/.*").setType(MatchType.REGEX)).
                        addMonitoredHttpClientEndpoint(new Match().setValue("localhost:8080"))
        );

        Vertx serververtx = Vertx.vertx(vopts);

        MetricsService serverMetricsService = MetricsService.create(serververtx);

        HttpServerOptions svroptions = new HttpServerOptions();
        svroptions.setHost("localhost");
        svroptions.setPort(8080);
        svroptions.getInitialSettings().setMaxConcurrentStreams(HTTP_SERVER_MAX_CONCURRENT_STREAMS);

        CountDownLatch deployLatch = new CountDownLatch(1);

        Verticle verticle = new AbstractVerticle() {
            private HttpServer httpServer;

            @Override
            public void start() throws Exception {

                this.vertx.createHttpServer(svroptions)
                        .requestHandler(ctx -> {
                            HttpServerResponse response = ctx.response();
                            response.setStatusCode(200);
                            response.end();
                        })
                        .listen(h -> {
                            if (h.succeeded()) {
                                httpServer = h.result();
                                deployLatch.countDown();
                            } else {
                                h.cause().printStackTrace();
                            }
                        });
            }

            @Override
            public void stop() throws Exception {
                if (this.httpServer != null) {
                    this.httpServer.close();
                }
            }

        };

        serververtx.deployVerticle(verticle, new DeploymentOptions());

        assertTrue(deployLatch.await(5, TimeUnit.SECONDS));

        Vertx vertx = Vertx.vertx(vopts);

        try {
            MetricsService clientMetricsService = MetricsService.create(vertx);

            CountDownLatch latch = new CountDownLatch(TOTAL_REQUESTS);

            WebClientOptions clientOptions = new WebClientOptions();
            clientOptions.setProtocolVersion(HttpVersion.HTTP_2);
            clientOptions.setHttp2ClearTextUpgrade(false);
            clientOptions.setHttp2MaxPoolSize(HTTP_CLIENT_MAX_CONNECTIONS);
            if (CLIENT_MULTIPLEX_MAX >= 0) {
                clientOptions.setHttp2MultiplexingLimit(CLIENT_MULTIPLEX_MAX);
            }

            WebClient client = WebClient.create(vertx, clientOptions);

            for (int i = 0; i < TOTAL_REQUESTS; i++) {
                HttpRequest<Buffer> request = client.request(HttpMethod.PUT, 8080, "localhost", "/nnrf-nfm/v1");
                request.putHeader("Content-Type", "application/json");

                request.sendBuffer(Buffer.buffer(json), (AsyncResult<HttpResponse<Buffer>> response) -> {
                    latch.countDown();
                });
            }

            assertTrue(latch.await(5, TimeUnit.SECONDS));

            JsonObject smetrics = serverMetricsService.getMetricsSnapshot("vertx");
            JsonObject cmetrics = clientMetricsService.getMetricsSnapshot("vertx");

            assertEquals((int) smetrics.getJsonObject("vertx.http.servers.localhost:8080.open-netsockets").getInteger("count"), EXPECTED_CONNECTIONS);
            assertEquals((int) cmetrics.getJsonObject("vertx.http.clients.endpoint.localhost:8080.open-netsockets").getInteger("count"), EXPECTED_CONNECTIONS);
            assertEquals((int) cmetrics.getJsonObject("vertx.http.clients.responses-2xx").getInteger("count"), TOTAL_REQUESTS);

            client.close();

        } finally {

            destroy(serververtx);
            destroy(vertx);
        }

    }

    public static void destroy(Vertx vertx) {
        if (vertx != null) {

            try {

                if (vertx.deploymentIDs().size() > 0) {
                    CountDownLatch latch = new CountDownLatch(vertx.deploymentIDs().size());
                    vertx.deploymentIDs().forEach(id -> {
                        vertx.undeploy(id, ch -> {
                            latch.countDown();
                        });
                    });

                    latch.await(10, TimeUnit.SECONDS);
                }

            } catch (Exception e) {
                //nothing
            }

            vertx.close();
        }

    }

}
