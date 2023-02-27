package com.hfu.kauz.resources;

import com.hfu.kauz.model.Measurement;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

/**
 * @author 1Zero64
 * Deserializer for Measurements and TransformedMeasurements Events for Kafka
 * @param <T>
 */
public class Deserializer<T> extends ObjectMapperDeserializer<Measurement> {

    public Deserializer() {
        super(Measurement.class);
    }
}
