package com.hfu.kauz.event_stream.kafka;

import com.hfu.kauz.benchmark.BenchmarkResource;
import com.hfu.kauz.database.DatabaseResource;
import com.hfu.kauz.model.Measurement;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.CompletionStage;

/**
 * @author 1Zero64
 * This class consumes measurements from the Kafka Cluster in the topic measurement-kafka-stream.
 * Itself and the methods Can be called with HTTP Requests and the right path. /kafka path must be included as the first part.
 */
@Path("kafka")
public class DataConsumer {

    // ArrayList to contain the process durations for every event
    ArrayList<Double> latencies = new ArrayList<>();
    // ArrayList to contain the overall elapsed time
    ArrayList<Double> times = new ArrayList<>();
    // Entry time point of the moment the application and consumer started
    double entryTime = System.currentTimeMillis();

    /**
     * @Inject DatabaseRessource: Class to utilize the database functions
     * BenchmarkResource: Class to display metrics for benchmarking the streaming systems
     */
    @Inject DatabaseResource databaseResource;
    @Inject BenchmarkResource benchmarkResource;

    // (JBoss) Logger: Logging bridge for the DataConsumer class to log messages and print them on console
    private final Logger logger = Logger.getLogger(DataConsumer.class);

    /**
     * Consumes the events from the Kafka topic measurement-kafka-stream via the channel in small batches and returns an acknowledgement.
     * @param messages - Message, which contains the measurements as a list in the payload and allows to access the metadata and handle the acknowledgement
     * @return CompletionStage - Response that the event is processed and not-/ acknowledged depending on acknowledgement strategy
     * @Incoming - Channel of measurement topic, where the events are read from
     * @Blocking - Indicates that the processing of the events is blocking and should not be run on the caller thread. E.g. when Reactive Messaging should be combined
     * with blocking processing like database interactions
     */
    @Incoming("measurement-incoming")
    @Blocking
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> consume(KafkaRecordBatch<String, Measurement> messages) {
        logger.infof("Kafka: Received a batch of %d messages", messages.getRecords().size());

        // Iterates through the message's payload, which contains the measurements, and process every measurement
        for (KafkaRecord<String, Measurement> record : messages) {

            // Capture starting point as a moment in the timeline
            Instant start = Instant.now();

            // Get measurement object from payload of the current record
            Measurement measurement = record.getPayload();

            // Log that Kafka received an event
            logger.infof("Kafka: Received event %s from batch of topic %s and partition %s", measurement, record.getTopic(), record.getPartition());

            // Process event by setting event_stream attribute and processed_on to the current date time with milliseconds
            measurement.setEvent_stream("Kafka");
            measurement.setProcessed_on(LocalDateTime.now());

            // Persist measurement with database service
            databaseResource.create(measurement);

            // Capture ending point as a moment in the timeline
            Instant end = Instant.now();
            // Calculate the elapsed time between the two moment to get the process-latency
            Duration timeElapsed = Duration.between(start, end);

            // Add the elapsed time in milliseconds to the list of durations
            latencies.add(((double) timeElapsed.toNanos()) / 1000000);

            // Add the overall elapsed time in seconds to the list of times
            times.add(((double) System.currentTimeMillis() - entryTime) / 1000);

            // Log the duration of th processing time
            logger.infof("Kafka: Time needed for consuming: %s", timeElapsed);
        }
        // Return CompletionStage and acknowledge the events
        return messages.ack();
    }

    /**
     * GET method to display the process latency benchmarks for every consumed event by Kafka consumer in a 2D plot
     * Triggered with an HTTP GET request and /processLatency path
     * Example call: localhost:8080/kafka/processLatency
     * @return  String display on browser with statistical information
     * @Produces Simple plain text with process latency statistics for display on the browser
     */
    @GET
    @Path("/processLatency")
    @Produces(MediaType.TEXT_PLAIN)
    public String displayProcessLatency() {

        // Call benchmark function to display the process latency
        return benchmarkResource.displayProcessLatency(latencies, times, "Kafka");
    }
}
