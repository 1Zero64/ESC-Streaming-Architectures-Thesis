package com.hfu.kauz.event_stream.kinesis;

import com.google.common.collect.Lists;
import com.hfu.kauz.model.Measurement;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hfu.kauz.datagenerator.DatageneratorResource;
import io.quarkus.scheduler.Scheduled;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 1Zero64
 * This class produces random generated measurements and sends them to Kinesis Data Streams into the stream measurement-kinesis-stream.
 *  @Path Path for HTTP API calls. Path starts with /kinesis.
 */
@Path("kinesis")
public class DataProducer {

    // (JBoss) Logger: Logging bridge for the DataConsumer class to log messages and print them on console
    private final Logger logger = Logger.getLogger(DataConsumer.class);

    // Build connection to Kinesis Client (access and secretAccess key for AWS user is needed)
    KinesisClient kinesisClient = getKinesisClient();

    // Create object mapper with time module for mapping events to measurements and serialize them into bytes
    static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    /**
     * @Inject Data generator for generating random measurements
     */
    @Inject
    DatageneratorResource datageneratorResource;

    /**
     * Method to produce random generated measurements and send them to the stream in Kinesis Data Streams
     * @Scheduled Execute the method every n seconds (default 1s = 1 second)
     */
    @Scheduled(every = "1s")
    public void produce() {

        // Create list for entries for a PutRecords request to Kinesis
        List<PutRecordsRequestEntry> putRecordsRequestList = new ArrayList<>();

        // Number of events/measurements to generate
        // Input a number higher than 1
        int numberOfEvents = 10;

        // Create and add n measurement entries to the request list with a loop
        for (int i = 0; i < numberOfEvents; i++) {

            // Generate random measurement event
            Measurement measurement = datageneratorResource.generateEvent();

            try {
                // Construct single entry for the PutRecords request
                PutRecordsRequestEntry putRecordsRequestEntry = PutRecordsRequestEntry.builder()
                        // Set partition key with hashed sensor_id to group events by shards
                        .partitionKey(String.format("partitionKey-%d", measurement.getSensor_id()))
                        // Write measurement as bytes (sdk bytes) with object mapper and SdkBytes and set is as the data blob (binary large object) in the request
                        .data(SdkBytes.fromByteArray(objectMapper.writeValueAsBytes(measurement)))
                        .build();

                // Add entry to the PutRecords request list
                putRecordsRequestList.add(putRecordsRequestEntry);
            } catch (JsonProcessingException e) {
                logger.error(String.format("Failed to serialize %s", measurement), e);
            }
        }

        // Decompose array list into array lists of max 500 items
        List<List<PutRecordsRequestEntry>> putRecordsRequestsList = Lists.partition(putRecordsRequestList, 500);

        // Iterate through list, construct and send PutRecordsRequest for every partitioned list
        for (List<PutRecordsRequestEntry> putRecordsList : putRecordsRequestsList) {
            // Construct PutRecords request to Kinesis
            PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder()
                    // Route request to measurement-kinesis-stream
                    .streamName(Configuration.streamName)
                    // Set the PutRecords request list with the entries as the data records
                    .records(putRecordsList)
                    .build();

            try {
                // Execute batched PutRecords request to Kinesis and handle exceptions
                PutRecordsResponse response = kinesisClient.putRecords(putRecordsRequest);
            } catch(KinesisException e) {
                logger.error("Failed to produce", e);
            }
        }
    }

    /**
     * @POST method to write a measurement into Kinesis
     * Triggered with an HTTP request and /kinesis path
     * Example call: http://localhost:8080/kinesis
     * @return String - Message as response
     * @Consumes JSON document of the generated measurement
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public String produceEvent(Measurement measurement) {

        try {
            // Construct single PutRecord request
            PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                    // Set partition key with hashed sensor_id to group events by shards
                    .partitionKey(String.valueOf(String.valueOf(measurement.getSensor_id()).hashCode()))
                    // Route request to measurement-kinesis-stream
                    .streamName(Configuration.streamName)
                    // Write measurement as bytes (sdk bytes) with object mapper and SdkBytes and set is as the data blob (binary large object) in the request
                    .data(SdkBytes.fromByteArray(objectMapper.writeValueAsBytes(measurement)))
                    .build();

            // Execute single PutRecord request to Kinesis and handle exceptions
            PutRecordResponse response = kinesisClient.putRecord(putRecordRequest);
        } catch (JsonProcessingException e) {
            logger.error(String.format("Failed to serialize %s", measurement), e);
        } catch (KinesisException e) {
            logger.error(String.format("Failed to produce %s", measurement), e);
        }

        // Return message as response
        return "Event received";
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
}