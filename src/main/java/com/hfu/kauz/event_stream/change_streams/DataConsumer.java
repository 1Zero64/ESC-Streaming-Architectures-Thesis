package com.hfu.kauz.event_stream.change_streams;

import com.hfu.kauz.benchmark.BenchmarkResource;
import com.hfu.kauz.database.DatabaseResource;
import com.hfu.kauz.model.Measurement;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
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

/**
 * @author 1Zero64
 * This class consumes changes from the Change Stream of the MongoDB Cluster.
 * Itself and the methods Can be called with HTTP Requests and the right path. /mongo path must be included as the first part.
 */
@Path("mongo")
public class DataConsumer {

    // ArrayList to contain the process durations for every event
    ArrayList<Double> latencies = new ArrayList<>();
    // ArrayList to contain the overall elapsed time
    ArrayList<Double> times = new ArrayList<>();
    // Entry time point of the moment the application and consumer started
    double entryTime;

    /**
     * @Inject MongoClient: Client to communicate with the MongoDB
     * DatabaseRessource: Class to utilize the database functions
     * BenchmarkResource: Class to display metrics for benchmarking the streaming systems
     */
    @Inject MongoClient mongoClient;
    @Inject DatabaseResource databaseResource;
    @Inject BenchmarkResource benchmarkResource;

    // (JBoss) Logger: Logging bridge for the DataConsumer class to log messages and print them on console
    private final Logger logger = Logger.getLogger(DataConsumer.class);

    /**
     * GET Method to trigger the subscription and Change Stream listener, to receive and process measurement events
     * Must be called with /stream path
     * E.g.: http://localhost:8080/mongo/stream
     */
    @GET
    @Path("/stream")
    public void consume() {

        // Set entry time
        entryTime = System.currentTimeMillis();

        // Subscribe to change streams in form of a cursor and watch for change stream document and events
        MongoCursor<ChangeStreamDocument<Document>> cursor = getCollection().watch().iterator();
        // Log info that consumer is listening to events
        logger.infof("Listening to events....");

        // Iterate through the cursor
        while (cursor.hasNext()) {

            // Get a change document from the stream when an event occured
            ChangeStreamDocument<Document> next = cursor.next();

            // Capture starting point as a moment in the timeline
            Instant start = Instant.now();

            // Log that Change Streams received an event
            logger.infof("Change Streams: Received an event: %s", next.getFullDocument());

            // Assert that document is not empty
            assert next.getFullDocument() != null;
            // Handle the full document with the measurement information of the event
            handleEvent(next.getFullDocument());

            // Capture ending point as a moment in the timeline
            Instant end = Instant.now();
            // Calculate the elapsed time between the two moment to get the process-latency
            Duration timeElapsed = Duration.between(start, end);

            // Add the elapsed time in milliseconds to the list of durations
            latencies.add(((double) timeElapsed.toNanos()) / 1000000);

            // Add the overall elapsed time in seconds to the list of times
            times.add(((double) System.currentTimeMillis() - entryTime) / 1000);

            // Log the duration of th processing time
            logger.infof("Change Streams: Time needed for consuming: %s", timeElapsed);
        }
    }

    /**
     * Handles and processes an occurred event in the change stream
     * @param document of a measurement that must be processed
     */
    private void handleEvent(Document document) {

        // Create empty measurement
        Measurement measurement = new Measurement();

        // Set attributes with the data from the document
        measurement.setSensor_id(document.getLong("sensor_id"));
        measurement.setTemperature(document.getDouble("temperature").floatValue());
        measurement.setHumidity(document.getDouble("humidity").floatValue());
        measurement.setCreated_on(LocalDateTime.parse(document.getString("created_on")));

        // Process event by setting event_stream attribute and processed_on to the current date time with milliseconds
        measurement.setEvent_stream("Change Streams");
        measurement.setProcessed_on(LocalDateTime.now());

        // Persist measurement with database service
        databaseResource.create(measurement);
    }

    /**
     * Returns a measurement collection from the measurement database of the Mongo Client
     * @return MongoCollection for measurements from the MongoClient
     */
    private MongoCollection getCollection() {

        // Return a measurement collection from the measurement database in the MongoDB
        return mongoClient.getDatabase("measurement").getCollection("measurement");
    }

    /**
     * GET method to display the process latency benchmarks for every consumed event by Change Streams consumer in a 2D plot
     * Triggered with an HTTP GET request and /processLatency path
     * Example call: localhost:8080/mongo/processLatency
     * @return  String display on browser with statistical information
     * @Produces Simple plain text with process latency statistics for display on the browser
     */
    @GET
    @Path("/processLatency")
    @Produces(MediaType.TEXT_PLAIN)
    public String displayProcessLatency() {

        // Call benchmark function to display the process latency
        return benchmarkResource.displayProcessLatency(latencies, times, "Change Streams");
    }
}
