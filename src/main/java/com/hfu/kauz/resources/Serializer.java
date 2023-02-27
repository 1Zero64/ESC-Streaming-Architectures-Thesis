package com.hfu.kauz.resources;

import com.hfu.kauz.model.Measurement;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

/**
 * @author 1Zero64
 * Deserializer for Measurements and TransformedMeasurements Events for Kafka
 * @param <T>
 */
public class Serializer<T> extends ObjectMapperSerializer<Measurement> {

    public Serializer() {
        super();
    }
}
