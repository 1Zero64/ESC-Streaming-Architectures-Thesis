package com.hfu.kauz.event_stream.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hfu.kauz.benchmark.BenchmarkResource;
import com.hfu.kauz.database.DatabaseResource;
import com.hfu.kauz.model.Measurement;
import io.quarkus.scheduler.Scheduled;
import org.jboss.logging.Logger;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.kinesis.model.Record;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 1Zero64
 * This class consumes measurements from the Kafka Cluster in the topic measurement-kafka-stream.
 * Itself and the methods Can be called with HTTP Requests and the right path. /kinesis path must be included as the first part.
 */
@Path("kinesis")
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

    // Get connection to Kinesis Client
    KinesisClient kinesisClient = getKinesisClient();

    // Create object mapper with time module for mapping events to measurement
    static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    // Build a request to get a description of a stream in Kinesis Data Streams
    DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
            // Route request to measurement-kinesis-stream
            .streamName(Configuration.streamName)
            .build();

    // Create an ArrayList for shard iterator pairs
    List<ShardIteratorPair> shardIterators = getShardIterators();

    /**
     * Consumes the events from the Kinesis data stream measurement-kinesis-stream's shards in small batches
     * @Scheduled Execute method every n seconds/minutes/hours/etc. (default 1s --> 1 second)
     */
    @Scheduled(every = "1s")
    public void consume() {

        // Iterate through every shard iterator pair in the list and consume the data
        for (ShardIteratorPair shardIteratorPair : shardIterators) {
            // Build request with shard iterator from the pair to receive the latest records in shard
            GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder()
                    // Set the shard iterator to read with
                    .shardIterator(shardIteratorPair.getShardIterator())
                    // Limit number of records to read from shard
                    .limit(1000)
                    .build();

            // Execute request with kinesis client to get the records in response
            GetRecordsResponse recordsResponse = kinesisClient.getRecords(getRecordsRequest);
            shardIteratorPair.setShardIterator(recordsResponse.nextShardIterator());

            // Check if records are found in the response
            if (recordsResponse.hasRecords()) {
                logger.infof("Kinesis: Received a batch of %d records from shard %s", recordsResponse.records().size(), shardIteratorPair.getShardId());

                // Iterate through all records in the response
                for (Record record : recordsResponse.records()) {

                    // Capture starting point as a moment in the timeline
                    Instant start = Instant.now();

                    // Get ByteArray of the record's data (the measurement as a data blob) as a string
                    String messungString = new String(record.data().asByteArray(), StandardCharsets.UTF_8);
                    // Initialize empty measurement object
                    Measurement measurement = new Measurement();

                    // Execute the main consumer part and handle exceptions
                    try {
                        // Map the string value to a measurement object with the object mapper
                        measurement = objectMapper.readValue(messungString, Measurement.class);

                        // Log that Kinesis received an event with shard and position information
                        logger.infof("Kinesis: Received event %s with partition key %s from shard %s at position %s", measurement, record.partitionKey(), shardIteratorPair.getShardId(), record.sequenceNumber());

                        // Process event by setting event_stream attribute and processed_on to the current date time with milliseconds
                        measurement.setEvent_stream("Kinesis");
                        measurement.setProcessed_on(LocalDateTime.now());

                    } catch (JsonProcessingException e) {
                        // Log when exception occurs
                        logger.error("Failed to deserialize record", e);
                    }

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
                    logger.infof("Kinesis: Time needed for consuming: %s", timeElapsed);
                }
            } else {
                // If no records are found log the information
                logger.infof("Kinesis: No new events founds.");
            }
        }
    }
    /**
     * Builds and returns a connection to a Kinesis Client (access and secretAccess key for AWS user is needed)
     * @return KinesisClient Connection
     */
    private KinesisClient getKinesisClient() {

        // Build and return connection to Kinesis Client
        return KinesisClient.builder()
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
    }

    /**
     * Return a list of all shards iterators and their Id as a pair
     * @return List of Pairs of found Shard Iterators in Kinesis Data Stream
     */
    private List<ShardIteratorPair> getShardIterators() {

        // Initialize an array list
        List<ShardIteratorPair> shardIterators = new ArrayList<>();

        // Get a response, with the description of the stream (e.g. shards), from the kinesis client with the stream request
        DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

        // Iterate through all shards in the stream description
        for (Shard shard : describeStreamResponse.streamDescription().shards()) {
            // Build a request to get a shard iterator for a shard
            GetShardIteratorRequest iteratorRequest = GetShardIteratorRequest.builder()
                    // Route request to measurement-kinesis-stream
                    .streamName(Configuration.streamName)
                    // Set type to latest, so the shard iterators starts reading just after the most recent record  in the shard, so that the most recent data in the shard is read
                    .shardIteratorType(ShardIteratorType.LATEST)
                    // Set the shard id to select a shard found in description
                    .shardId(shard.shardId())
                    .build();
            // Get shard iterator as response from kinesis client with the iterator request
            GetShardIteratorResponse shardIteratorResponse = kinesisClient.getShardIterator(iteratorRequest);

            // Get the shard iterator from the response as a string
            String shardIterator = shardIteratorResponse.shardIterator();

            // Add a new shard iterator pair with shard id and the iterator in the string
            shardIterators.add(new ShardIteratorPair(shard.shardId(), shardIterator));
            }

        // Return the array list with the shard iterators
        return shardIterators;
    }

    /**
     * GET method to display the process latency benchmarks for every consumed event by Kinesis consumer in a 2D plot
     * Triggered with an HTTP GET request and /processLatency path
     * Example call: localhost:8080/kinesis/processLatency
     * @return  String display on browser with statistical information
     * @Produces Simple plain text with process latency statistics for display on the browser
     */
    @GET
    @Path("/processLatency")
    @Produces(MediaType.TEXT_PLAIN)
    public String displayProcessLatency() {

        // Call benchmark function to display the process latency
        return benchmarkResource.displayProcessLatency(latencies, times, "Kinesis");
    }
}