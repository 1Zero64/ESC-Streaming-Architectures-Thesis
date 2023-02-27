package com.hfu.kauz.event_stream.kinesis;

/**
 * @author 1Zero64
 * Object class for a shard iterator pair for handling shards with Kinesis consumer.
 */
public class ShardIteratorPair {

    // Id of a found shard
    private String shardId;
    // Shard iterator of a found shard
    private String shardIterator;

    // Constructor for a shard iterator pair
    public ShardIteratorPair(String shardId, String shardItr) {
        this.shardId = shardId;
        this.shardIterator = shardItr;
    }

    // Default constructor
    public ShardIteratorPair() {

    }

    // Getter and Setter methods
    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getShardIterator() {
        return shardIterator;
    }

    public void setShardIterator(String shardIterator) {
        this.shardIterator = shardIterator;
    }

    /**
     * Overwritten toString method to display a shard iterator pair on console with its information
     * @return String to display object on console
     */
    @Override
    public String toString() {
        return "ShardIteratorPair{" +
                "shardId='" + shardId + '\'' +
                ", shardIterator='" + shardIterator + '\'' +
                '}';
    }
}
