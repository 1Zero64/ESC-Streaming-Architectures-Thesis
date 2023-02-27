package com.hfu.kauz.event_stream.change_streams;

import com.hfu.kauz.model.Measurement;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.time.LocalDateTime;
import java.util.ArrayList;

/**
 * @author 1Zero64
 * Class for MongoDB interactions and querying.
 * Itself and the methods can be called with HTTP Requests and the right path. /mongo path must be included as the first part.
 */
@Path("mongo")
public class MongoService {

    /**
     * @Inject MongoClient to communicate with the MongoDB
     */
    @Inject MongoClient mongoClient;

    /**
     * GET method to retrieve measurements in MongoDB
     * Triggered with an HTTP request and /read path
     * @return list of measurements found in MongoDB
     * Example call: http://localhost:8080/mongo/read
     */
    @GET
    @Path("/read")
    public ArrayList<Measurement> read() {

        // Prepare a ArrayList of measurements
        ArrayList<Measurement> measurements = new ArrayList<>();
        // Get a mongo cursor iterator for the measurement collection
        MongoCursor<Document> cursor = getCollection().find().iterator();

        // Iterate through mongo cursor, convert documents to measurements and add them to the list
        try {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                Measurement measurement = new Measurement();
                measurement.setSensor_id(document.getLong("sensor_id"));
                measurement.setTemperature(document.getDouble("temperature").floatValue());
                measurement.setHumidity(document.getDouble("humidity").floatValue());
                measurement.setCreated_on(LocalDateTime.parse(document.getString("created_on")));

                measurements.add(measurement);
            }
        // Close the cursor in the end
        } finally {
            cursor.close();
        }

        // Return the list of measurements
        return measurements;
    }

    /**
     * DELETE Method to delete all measurements in MongoDB measurement collection
     * Triggered with an HTTP request and /delete path
     * @Produces Simple plain text for display on the browser
     * @return confirmation String as response that the collection was cleaned successfully
     * Example call: http://localhost:8080/mongo/delete
     */
    @DELETE
    @Path("/delete")
    @Produces(MediaType.TEXT_PLAIN)
    public String delete() {

        getCollection().deleteMany(new Document());
        // Response message
        return "MongoDB collection cleaned";
    }

    /**
     * Returns a measurement collection from the measurement database in the MongoDB with the Mongo Client
     * @return MongoCollection for measurements from the MongoClient
     */
    private MongoCollection<Document> getCollection() {

        return mongoClient.getDatabase("measurement").getCollection("measurement");
    }
}
