package com.hfu.kauz.database;

import com.hfu.kauz.model.Measurement;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.transaction.Transactional;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * @author 1Zero64
 * Class for database interactions and querying (CRUD-Operations).
 * Itself and the methods can be called with HTTP Requests and the right path. /database path must be included as the first part.
 */
@Path("database")
@ApplicationScoped
public class DatabaseResource {

     /**
     * @Inject EntityManager: API to create and remove, find and query persistent entity instances
     */
    @Inject EntityManager entityManager;

    // (JBoss) Logger: Logging bridge for the DatabaseResource class to log messages and print them on console
    Logger logger = Logger.getLogger(DatabaseResource.class);

    /**
     * GET method to retrieve measurements in database
     * Triggered with an HTTP request and /read path
     * @return list of measurements found in database ordered by measurement.id ascending
     * Example call: http://localhost:8080/database/read
     */
    @GET
    @Path("/read")
    public List<Measurement> read() {

        return entityManager.createQuery("FROM Measurement e ORDER BY id", Measurement.class).getResultList();
    }

    /**
     * DELETE Method to delete all measurements in event_store table in database
     * Triggered with an HTTP request and /delete path
     * @Produces Simple plain text for display on the browser
     * @return confirmation String as response that the event_store table was cleaned successfully
     * Example call: http://localhost:8080/database/delete
     */
    @DELETE
    @Path("/delete")
    @Produces(MediaType.TEXT_PLAIN)
    @Transactional
    public String deleteAll() {

        entityManager.createQuery("DELETE FROM Measurement").executeUpdate();
        // Response message
        return "Table event_store cleaned";
    }

    /**
     * POST Method to persist a measurement in the databases' event_store table
     * Triggered with an HTTP request and /create path and body
     * @param measurement to be persisted
     * Example call: http://localhost:8080/database/create
     */
    @POST
    @Path("/create")
    @Transactional
    public void create(Measurement measurement) {

        entityManager.persist(measurement);
    }
}
