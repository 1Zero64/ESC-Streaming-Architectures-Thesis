package com.hfu.kauz.model;

import javax.persistence.*;
import java.time.LocalDateTime;

/**
 * @author 1Zero64
 * Entity-class for a transformed measurement with the table name "materialized_view" in the configured Postgres database.
 * This is the example use case for the materialize part.
 */
@Entity
@Table(name = "materialized_view")
public class TransformedMeasurement {

    // Unique identifier and primary key for a transformed measurement object
    @Id
    private long id;
    // Unique identifier of the sensor, that "measured" the measurement
    private long sensor_id;
    // Measured temperature in Grad Celsius (e.g. -1°C)
    private float temperature;
    // Measured humidity in percentage (e.g. 12%)
    private float humidity;
    // Name of the streaming technology that was used to stream the measurement. For filtering and querying purposes
    private String event_stream;
    // Date and time with milliseconds as a timestamp on when the measurement was created
    private LocalDateTime created_on;
    // Date and time with milliseconds as a timestamp on when the measurement was processed by the event stream and event handler (the consumer)
    private LocalDateTime processed_on;
    // Danger level of a measurement and state of the cold storage. Dependent on measured temperature and humidity
    private String danger;
    // Duration for processing a measurement event between creation timestamp and processing timestamp
    private float latency;

    // Transformed measurement constructor with measurement as parameter and basis
    public TransformedMeasurement(Measurement measurement) {
        setSensor_id(measurement.getSensor_id());
        setTemperature(measurement.getTemperature());
        setHumidity(measurement.getHumidity());
        setEvent_stream(measurement.getEvent_stream());
        setCreated_on(measurement.getCreated_on());
        setProcessed_on(measurement.getProcessed_on());
    }

    // Default constructor
    public TransformedMeasurement() {

    }

    // Getters and Setters for attributes

    /**
     * @Id - Just a mark that these are related to the Id attribute
     * @SequenceGenerator - Defines the unique primary key generator "messungSeq", which points to the "messung_id_seq" object in the database and can be referenced
     * - name: Unique name of generator
     * - sequenceName: Object in database
     * - allocationSize (optional but recommended): Amount to increment by when allocating sequence numbers
     * - initialValue (optional): Value from which the sequence object starts to generate and increment (default is 1)
     * @GeneratedValue - References and uses the "messungSeq" generator to generate the value and set the id attribute
     */
    @Id
    @SequenceGenerator(name = "messungSeq", sequenceName = "messung_id_seq", allocationSize = 1, initialValue = 1)
    @GeneratedValue(generator = "messungSeq")
    public long getId() {
        return id;
    }

    public void setId(long id) {
        id = id;
    }

    public long getSensor_id() {
        return sensor_id;
    }

    public void setSensor_id(long sensorId) {
        this.sensor_id = sensorId;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    public float getHumidity() {
        return humidity;
    }

    public void setHumidity(float humidity) {
        this.humidity = humidity;
    }

    public String getEvent_stream() {
        return event_stream;
    }

    public void setEvent_stream(String event_stream) {
        this.event_stream = event_stream;
    }

    public LocalDateTime getCreated_on() {
        return created_on;
    }

    public void setCreated_on(LocalDateTime timestamp) {
        this.created_on = timestamp;
    }

    public LocalDateTime getProcessed_on() {
        return processed_on;
    }

    public void setProcessed_on(LocalDateTime processed_on) {
        this.processed_on = processed_on;
    }

    public String getDanger() {
        return danger;
    }

    public void setDanger(String danger) {
        this.danger = danger;
    }

    public float getLatency() {
        return latency;
    }

    public void setLatency(float latency) {
        this.latency = latency;
    }

    /**
     * Overwritten toString method for printing the object on console
     * @return Object information as String to display it on console
     */
    @Override
    public String toString() {
        return "MeasurementTransformed{" +
                "id=" + id +
                ", sensor_id=" + sensor_id +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                ", event_stream='" + event_stream + '\'' +
                ", created_on=" + created_on +
                ", processed_on=" + processed_on +
                ", danger='" + danger + '\'' +
                ", latency=" + latency +
                '}';
    }
}