package com.hfu.kauz.datagenerator;

import com.hfu.kauz.model.Measurement;

import javax.enterprise.context.ApplicationScoped;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author 1Zero64
 * Resource to generate data for the producers.
 */
@ApplicationScoped
public class DatageneratorResource {

    /**
     * Static method to generate and return random measurement events.
     * @return a measurement object with random generated values as the payload
     */
    public Measurement generateEvent() {

        // Use Random
        Random random = new Random();

        // Generate random values
        long sensorId = ThreadLocalRandom.current().nextLong(1, 4);
        float temperature = (float) (-5.0 + (15.0 - (-5.0)) * random.nextFloat());
        float humidity = (float) (0.0 + (70.0 - (0.0)) * random.nextFloat());

        // Create and set the payload (the measurement) with the random values and the current date and time with milliseconds
        Measurement payloadToSend = new Measurement();
        payloadToSend.setSensor_id(sensorId);
        payloadToSend.setTemperature(temperature);
        payloadToSend.setHumidity(humidity);
        payloadToSend.setCreated_on(LocalDateTime.now());

        // Return the measurement
        return payloadToSend;
    }
}
