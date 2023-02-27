package com.hfu.kauz.materializer;

import com.hfu.kauz.model.Danger;
import com.hfu.kauz.model.Measurement;
import com.hfu.kauz.model.TransformedMeasurement;
import org.jboss.logging.Logger;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.transaction.Transactional;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * @author 1Zero64
 * Class for materializer and its interactions and querying (CRUD-Operations)
 * Itself and the methods can be called with HTTP Requests and the right path. /materializer path must be included as the first part
 */
@Path("materializer")
public class MaterializerResource {

    /**
     * @Inject EntityManager: API to create and remove, find and query persistent entity instances
     */
    @Inject EntityManager entityManager;

    // (JBoss) Logger: Logging bridge for the DatabaseResource class to log messages and print them on console
    Logger logger = Logger.getLogger(MaterializerResource.class);

    /**
     * GET method to retrieve transformed measurements in database
     * Triggered with an HTTP request and /get path
     * @return list of transformed measurements found in database
     * Example call: http://localhost:8080/materializer/read
     */
    @GET
    @Path("/read")
    public List<TransformedMeasurement> get() {

        // Prepare query
        String readQuery = "FROM TransformedMeasurement";
        // Execute and Update on database and return the retrieved list with mapped transformed measurement objects
        return entityManager.createQuery(readQuery, TransformedMeasurement.class).getResultList();
    }

    /**
     * GET method to execute the materialize process and write data from event store to materialized view
     * Triggered with an HTTP request and /materialize path
     * @Produces Simple plain text for display on the browser
     * @return confirmation string display on browser with additional information, that the Materializer finished successfully
     * Example call: http://localhost:8080/materializer/materialize
     */
    @GET
    @Path("/materialize")
    @Produces(MediaType.TEXT_PLAIN)
    public String materializeView() {

        // Save start point
        LocalDateTime start = LocalDateTime.now();

        // Run the materialize process and get the number of processed measurements
        int numberOfMeasurements = materialize();

        // Save end time and calculate difference between start and end time to calculate the materialize process time
        LocalDateTime end = LocalDateTime.now();
        double timeNeeded = (double) ChronoUnit.MILLIS.between(start, end) / 1000;

        // Return confirmation string with start and times as well as the number of rows processed and the time needed for that displayed to the browser
        return "Materializer finished: " + end + "\n" +
                "Started: " + start + "\n" +
                "Datapoints processed: " + numberOfMeasurements + "\n" +
                "Time needed: " + timeNeeded + " seconds";
    }

    /**
     * Method to control the materialize process
     * @return Number of transformed measurements
     */
    public int materialize() {

        // Clean materialized view in database
        cleanMaterializedView();

        // Initialize ArrayList for transformed measurements
        ArrayList<TransformedMeasurement> transformedMeasurements = new ArrayList<>();

        // Read measurements in event store into an ArrayList
        ArrayList<Measurement> measurements = readMessungen();

        // Iterate through found measurements, transform them and add them to the ArrayList for transformed measurements
        for (Measurement measurement : measurements) {
            transformedMeasurements.add(transformMeasurement(measurement));
        }

        // Write transformed measurements into the databases materialized_view table
        writeMessungenTransformed(transformedMeasurements);

        // Return number of transformed measurements
        return measurements.size();
    }

    /**
     * Method to read all measurement from event_store in database and return them as a list
     * @return ArrayList of all read measurements
     */
    public ArrayList<Measurement> readMessungen() {

        // Prepare read query
        String readQuery = "FROM Measurement ORDER BY id ASC";
        // Execute Query and return list of the measurements
        return (ArrayList<Measurement>) entityManager.createQuery(readQuery).setMaxResults(1000).getResultList();
    }

    /**
     * DELETE Method to clean the materialized view table in the database
     */
    @Transactional
    public void cleanMaterializedView() {

        // Prepare Clean-Query
        String cleanQuery = "DELETE FROM TransformedMeasurement";
        // Execute Query and Update on database
        entityManager.createQuery(cleanQuery).executeUpdate();
    }

    /**
     * Method to persist every transformed measurement in the database
     * @param transformedMeasurements list of all transformed measurements
     */
    @Transactional
    public void writeMessungenTransformed(ArrayList<TransformedMeasurement> transformedMeasurements) {

        // Iterate through list and persist every transformed measurement with entity manager
        for (TransformedMeasurement transformedMeasurement : transformedMeasurements) {
            entityManager.persist(transformedMeasurement);
        }
    }

    /**
     * Transform a measurement by calculating and setting latency in milliseconds and danger level
     * @param measurement the measurement that has to be transformed
     * @return transformed measurement with danger and latency
     */
    public TransformedMeasurement transformMeasurement(Measurement measurement) {

        // Prepare transformed measurement with base data
        TransformedMeasurement transformedMeasurement = new TransformedMeasurement(measurement);

        // Calculate latency between creation datetime and processed datetime to get it in Nanoseconds then divide it by 1.000.000 to get Milliseconds
        float latency = (float) ChronoUnit.NANOS.between(transformedMeasurement.getCreated_on(), transformedMeasurement.getProcessed_on()) / 1000000;
        transformedMeasurement.setLatency(latency);

        // Set danger level by traversing through if-statements, that check temperature and humidity
        if (transformedMeasurement.getTemperature() > 10
                || transformedMeasurement.getHumidity() > 60) {
            transformedMeasurement.setDanger(Danger.Critical.name());

        } else if (transformedMeasurement.getTemperature() > 7
                || transformedMeasurement.getHumidity() > 50) {
            transformedMeasurement.setDanger(Danger.High.name());

        } else if (transformedMeasurement.getTemperature() > 5
                || transformedMeasurement.getHumidity() > 40) {
            transformedMeasurement.setDanger(Danger.Medium.name());

        } else if (transformedMeasurement.getTemperature() > 3
                || transformedMeasurement.getHumidity() > 20) {
            transformedMeasurement.setDanger(Danger.Low.name());

        } else {
            transformedMeasurement.setDanger(Danger.No.name());
        }

        // Return transformed measurement
        return transformedMeasurement;
    }

    /**
     * GET method to execute the materialize process multiple times for a microbenchmark
     * Triggered with an HTTP request and /microbenchmark/{iterations} path
     * @Produces Simple plain text for display on the browser
     * @return string display on browser with microbenchmark information of the materializer
     * Example call: http://localhost:8080/materializer/microbenchmark/11
     */
    @GET
    @Path("/microbenchmark/{iterations}")
    @Produces(MediaType.TEXT_PLAIN)
    public String microbenchmark(@PathParam("iterations") int iterations) {

        // Print information about starting the test
        logger.infof("Starting microbenchmark...");

        // Number of processed datapoints
        int numberOfMeasurements = 0;

        // Array list for each iteration duration
        ArrayList<Double> iterationDurations = new ArrayList<>();

        for (int i = 0; i < iterations; i++) {
            // Save start point
            LocalDateTime start = LocalDateTime.now();

            // Run the materialize process
            numberOfMeasurements = materialize();

            // Save end time and calculate difference between start and end time to calculate the materialize process time
            LocalDateTime end = LocalDateTime.now();
            double timeNeeded = (double) ChronoUnit.MILLIS.between(start, end) / 1000;

            // Add duration to list
            iterationDurations.add(timeNeeded);

            logger.infof("Iteration %d/%d finished", (i+1), iterations);
        }

        // Make copy of unordered list
        ArrayList<Double> unorderedIterationDurations = (ArrayList<Double>) iterationDurations.clone();

        // Sort array list
        Collections.sort(iterationDurations);

        // Calculate average duration
        float averageDuration = (float) iterationDurations.stream().mapToDouble(duration -> duration).average().orElse(0.0);

        // Calculate median duration
        float medianDuration = 0;
        // For even iterations
        if (iterations % 2 == 0) {
            medianDuration = (float) (iterationDurations.get(iterations/2) + iterationDurations.get(iterations/2 - 1))/2;
        // For odd iterations
        } else {
            medianDuration = (iterationDurations.get(iterations/2)).floatValue();
        }

        // Calculate variance and standard deviation
        float sum = 0;

        // Find sum of square distances to the mean for every duration
        for (int i = 0; i < iterationDurations.size(); i++) {
            float squareDiffToMean = (float) Math.pow(iterationDurations.get(i) - averageDuration, 2);
            sum += squareDiffToMean;
        }

        // Divide sum by number of iterations to get variance
        float variance = sum / (float) iterationDurations.size();
        // Take square root for standard deviation
        float standardDeviation = (float) Math.sqrt(variance);

        // Print information about finished test
        logger.infof("Microbenchmark finished");

        // Return display string with microbenchmark statistics to the browser
        return "Java Materializer Microbenchmark\n" +
                "Number of iterations:\t\t" + iterations + "\n" +
                "Datapoints processed each:\t" + numberOfMeasurements + "\n" +
                "Fastest iteration (min):\t" + iterationDurations.get(0) + " seconds\n" +
                "Slowest iteration (max):\t" + iterationDurations.get(iterationDurations.size()-1) + " seconds\n" +
                "Average duration (avg/mean):\t" + averageDuration + " seconds\n" +
                "Median duration (median):\t" + medianDuration + " seconds\n" +
                "Standard deviation:\t\t" + standardDeviation + " seconds\n" +
                "Variance:\t\t\t" + variance + " seconds\n\n\n" +
                "All runs:\n" + iterationDurations + "\n\n" +
                "All runs (unsorted):\n" + unorderedIterationDurations;
    }

    /**
     * GET method to execute the microbenchmark 11 times by default
     * Triggered with an HTTP request and /microbenchmark path
     * @Produces Simple plain text for display on the browser
     * @return string display on browser with microbenchmark information of the materializer
     * Example call: http://localhost:8080/materializer/microbenchmark
     */
    @GET
    @Path("/microbenchmark")
    @Produces(MediaType.TEXT_PLAIN)
    public String callMicrobenchmark() {
        return microbenchmark(11);
    }

}