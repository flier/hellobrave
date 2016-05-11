import com.github.kristofa.brave.*;
import com.github.kristofa.brave.internal.Nullable;
import com.github.kristofa.brave.kafka.KafkaSpanCollector;
import com.google.common.net.InetAddresses;
import com.twitter.zipkin.gen.Endpoint;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class Sample {
    static final Logger logger = LoggerFactory.getLogger(Sample.class);

    static final String OPT_HELP = "help";
    static final String OPT_KAFKA_HOST = "kafka-host";
    static final String OPT_KAFKA_TOPIC = "kafka-topic";
    static final String OPT_SERVICE_NAME = "service-name";
    static final String OPT_SAMPLE_RATE = "sample-rate";

    static final String DEFAULT_KAFKA_HOST = "127.0.0.1:9092";
    static final String DEFAULT_KAFKA_TOPIC = "tracing";
    static final String DEFAULT_SERVICE_NAME = "hello-brave";

    private final Random rand = new Random();
    private Brave brave;
    private SpanId spanId;

    private CommandLine parseCmdLine(String[] args) throws ParseException {
        Options options = new Options();

        options.addOption(Option.builder("h")
                .longOpt(OPT_HELP)
                .desc("Show this help screen")
                .build());
        options.addOption(Option.builder("k")
                .longOpt(OPT_KAFKA_HOST)
                .hasArg()
                .argName("uri")
                .desc("Bootstrap broker(s) (host[:port])")
                .build());
        options.addOption(Option.builder("t")
                .longOpt(OPT_KAFKA_TOPIC)
                .hasArg()
                .argName("name")
                .desc("Topic to produce to (default: " + DEFAULT_KAFKA_TOPIC + ")")
                .build());
        options.addOption(Option.builder("n")
                .longOpt(OPT_SERVICE_NAME)
                .hasArg()
                .argName("name")
                .desc("Service name (default: " + DEFAULT_SERVICE_NAME + ")")
                .build());
        options.addOption(Option.builder("s")
                .longOpt(OPT_SAMPLE_RATE)
                .hasArg()
                .argName("rate")
                .type(float.class)
                .desc("Sampling rate (default: 1.0)")
                .build());

        CommandLineParser parser = new DefaultParser();

        final CommandLine opts = parser.parse(options, args);

        if (opts.hasOption('h')) {
            new HelpFormatter().printHelp( "ant", options, true);

            System.exit(0);
        }

        final String kafkaHost = opts.getOptionValue(OPT_KAFKA_HOST, DEFAULT_KAFKA_HOST);
        final String kafkaTopic = opts.getOptionValue(OPT_KAFKA_TOPIC, DEFAULT_KAFKA_TOPIC);

        final SpanCollectorMetricsHandler metrics = new SpanCollectorMetricsHandler() {
            @Override
            public void incrementAcceptedSpans(int quantity) {

            }

            @Override
            public void incrementDroppedSpans(int quantity) {

            }
        };

        final KafkaSpanCollector collector = KafkaSpanCollector.create(
                KafkaSpanCollector.Config.builder(kafkaHost).topic(kafkaTopic).build(), metrics);

        logger.info("sending tracing span to kafka://{}#{}", kafkaHost, kafkaTopic);

        final String serviceName = opts.getOptionValue(OPT_SERVICE_NAME, DEFAULT_SERVICE_NAME);
        final Sampler sampler = opts.hasOption('s') ?
                Sampler.create(Float.parseFloat(opts.getOptionValue(OPT_SAMPLE_RATE))) : Sampler.ALWAYS_SAMPLE;

        brave = new Brave.Builder(serviceName)
                .traceSampler(sampler)
                .spanCollector(collector)
                .build();

        logger.info("tracing `{}` service in {}% sample rate", serviceName,
                sampler == Sampler.ALWAYS_SAMPLE ? "100" : Float.parseFloat(opts.getOptionValue(OPT_SAMPLE_RATE))*100);

        return opts;
    }

    private void clientSendRequest() throws UnknownHostException, InterruptedException {
        final int addr = InetAddresses.coerceToInteger(Inet4Address.getLocalHost());
        final Endpoint endpoint = Endpoint.create(DEFAULT_SERVICE_NAME, addr, 5000);

        logger.info("client sent request from {}@{}", endpoint.service_name, InetAddresses.fromInteger(addr));

        brave.clientRequestInterceptor().handle(new ClientRequestAdapter() {
            @Override
            public String getSpanName() {
                return "client-call";
            }

            @Override
            public void addSpanIdToRequest(@Nullable SpanId spanId) {
                logger.info("request span {}", spanId);

                Sample.this.spanId = spanId;
            }

            @Override
            public Collection<KeyValueAnnotation> requestAnnotations() {
                return new ArrayList<KeyValueAnnotation>() {{
                    add(KeyValueAnnotation.create("foo", "bar"));
                }};
            }

            @Override
            public Endpoint serverAddress() {
                return endpoint;
            }
        });
    }

    private void clientReceiveResponse() throws InterruptedException {
        brave.clientResponseInterceptor().handle(new ClientResponseAdapter() {
            @Override
            public Collection<KeyValueAnnotation> responseAnnotations() {
                return Collections.EMPTY_LIST;
            }
        });
    }

    private void serverReceiveRequest() throws InterruptedException {
        final SpanId spanId = SpanId.builder().traceId(Sample.this.spanId.traceId).spanId(rand.nextLong()).build();
        final TraceData traceData = TraceData.builder().spanId(spanId).build();

        logger.info("server received request {}", spanId);

        brave.serverRequestInterceptor().handle(new ServerRequestAdapter() {
            @Override
            public TraceData getTraceData() {
                return traceData;
            }

            @Override
            public String getSpanName() {
                return "server-call";
            }

            @Override
            public Collection<KeyValueAnnotation> requestAnnotations() {
                return Collections.EMPTY_LIST;
            }
        });
    }

    private void serverSendResponse() throws InterruptedException {
        brave.serverResponseInterceptor().handle(new ServerResponseAdapter() {
            @Override
            public Collection<KeyValueAnnotation> responseAnnotations() {
                return Collections.EMPTY_LIST;
            }
        });
    }

    public static void main(String[] args) throws Exception {
        final Sample sample = new Sample();

        sample.parseCmdLine(args);

        final Semaphore clientRequestSent = new Semaphore(0);
        final Semaphore serverResponseSent = new Semaphore(0);

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sample.clientSendRequest();

                    Thread.sleep(sample.rand.nextInt(100));

                    logger.info("waiting server response");

                    clientRequestSent.release();

                    serverResponseSent.acquire();

                    Thread.sleep(sample.rand.nextInt(100));

                    sample.clientReceiveResponse();

                    logger.info("client received response");
                } catch (Exception e) {
                    logger.error("client crashed", e);
                } finally {
                    clientRequestSent.release();
                }
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    logger.info("waiting client request");

                    clientRequestSent.acquire();

                    sample.serverReceiveRequest();

                    Thread.sleep(sample.rand.nextInt(100));

                    sample.serverSendResponse();

                    logger.info("server sent response");
                } catch (Exception e) {
                    logger.error("server crashed", e);
                } finally {
                    serverResponseSent.release();
                }
            }
        });

        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
