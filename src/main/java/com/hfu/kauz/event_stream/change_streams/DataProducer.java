package com.hfu.kauz.event_stream.change_streams;

import com.hfu.kauz.datagenerator.DatageneratorResource;
import com.hfu.kauz.model.Measurement;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.quarkus.scheduler.Scheduled;
import org.bson.Document;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

/**
 * @author 1Zero64
 * This class produces random generated measurements and sends them to the MongoDB Cluster to persist them and make changes.
 * @Path Path for HTTP API calls. Path starts with /kinesis.
 */
@Path("mongo")
public class DataProducer {

    /**
     * @Inject MongoClient to communicate with the MongoDB
     */
    @Inject MongoClient mongoClient;

    /**
     * @Inject Data generator for generating random measurements
     */
    @Inject DatageneratorResource datageneratorResource;

    /**
     * Method to produce random generated measurements and send them to a collection in MongoDB
     * @Scheduled Execute the method every n seconds (default 1s = 1 second)
     */
    @Scheduled(every = "1s")
    public void produce() {

        // Number of events/measurements to generate
        // Input a number higher than 1
        int numberOfEvents = 0;

        // Generate and write n measurement documents to a collection in a loop
        for (int i = 0; i < numberOfEvents; i++) {

            // Prepare a measurement in form of a document
            Document document = prepareDocument();

            // Write the document into a collection
            getCollection().insertOne(document);
        }
    }

    /**
     * Prepares and returns a document of a randomly generated measurement
     * @return measurement document
     */
    private Document prepareDocument() {

        // Generate random measurement with the datagenerator
        Measurement measurement = datageneratorResource.generateEvent();

        // Create and return a document with the data from the generated measurement
        return new Document()
                .append("id", measurement.getId())
                .append("sensor_id", measurement.getSensor_id())
                .append("temperature", measurement.getTemperature())
                .append("humidity", measurement.getHumidity())
                .append("created_on", String.valueOf(measurement.getCreated_on()));
    }

    /**
     * @POST method to write a measurement into MongoDB
     * Triggered with an HTTP request and /mongo path
     * Example call: http://localhost:8080/mongo
     * @return String - Message as response
     * @Consumes JSON document of the generated measurement
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public String produceEvent(Measurement measurement) {

        // Write measurement into the collection
        getCollection().insertOne(new Document()
                .append("id", measurement.getId())
                .append("sensor_id", measurement.getSensor_id())
                .append("temperature", measurement.getTemperature())
                .append("humidity", measurement.getHumidity())
                .append("created_on", String.valueOf(measurement.getCreated_on())));

        // Return message as response
        return "Event received";
    }

    /**
     * Returns a measurement collection from a database of the Mongo Client
     * @return MongoCollection for measurements from the MongoClient
     */
    private MongoCollection<Document> getCollection() {

        // Return a measurement collection from the measurement database in the MongoDB
        return mongoClient.getDatabase("measurement").getCollection("measurement");
    }
}
