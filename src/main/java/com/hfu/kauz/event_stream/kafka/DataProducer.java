package com.hfu.kauz.event_stream.kafka;

import com.hfu.kauz.model.Measurement;
import com.hfu.kauz.datagenerator.DatageneratorResource;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.*;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * @author 1Zero64
 * This class produces random generated measurements and sends them to the Kafka Cluster into the topic measurement-kafka-stream.
 * @Path Path for HTTP API calls. Path starts with /kafka.
 */
@Path("kafka")
public class DataProducer {

    /**
     * @Inject Emitter to send Measurement events from imperative code to reactive messaging.
     * E.g. when receiving a POST request inside a REST endpoint
     * @Channel Bound emitter to outgoing channel of the measurement topic, so it sends events to the right stream
     */
    @Inject
    @Channel("measurement-outgoing")
    Emitter<Measurement> emitter;

    /**
     * @Inject Data generator for generating random measurements
     */
    @Inject DatageneratorResource datageneratorResource;

    // Number of events/measurements to generate
    // Input a number higher than 1
    int numberOfEvents = 1;

    /**
     * Generates a random measurement object and sends it to the Kafka topic every n milliseconds (in this case every 100 Millis) via the channel and returns a Multi.
     * @return Multi - Implements the reactive streams Publisher interface. Used by the Smallrye Framework to generate events and send them to the Kafka topic
     * @Outgoing - Channel of measurement topic, where the events are written to measurement topic, where the events are written to
     */
    @Outgoing("measurement-outgoing")
    public Multi<Message<Measurement>> produce() {
        // Create measurement event to send with Multi
        return Multi.createFrom()
                // Tick the sending of an event every n Millis
                .ticks().every(Duration.ofMillis(1000 / numberOfEvents))
                // Map a message of a randomly generated measurement to the Multi
                .map(m -> Message.of(datageneratorResource.generateEvent()));
    }

    /**
     * @POST method to write a measurement into Kafka topic via Emitter
     * Triggered with an HTTP request and /kafka path
     * Example call: http://localhost:8080/kafka
     * @return String - Message as response
     * @Consumes JSON document of the generated measurement
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public String produceEvent(Measurement measurement) {

        // Send measurement event as message to topic with emitter and return the completion status of computation
        emitter.send(Message.of(measurement)
                // Add partition key with metadata
                .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                        // Set int of sensor id modulo 3 (because there are only 3 partitions) as partition key
                        .withPartition((int) measurement.getSensor_id() % 3)
                        .build())
                .withAck(() -> {
                    // Called when the message is acknowledged
                    return CompletableFuture.completedFuture(null);
                })
                .withNack(throwable -> {
                    // Called when the message is not acknowledged
                    return CompletableFuture.completedFuture(null);
                }));

        // Return message as response
        return "Event received";
    }
}
