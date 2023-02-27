package com.hfu.kauz.benchmark;

import org.math.plot.Plot2DPanel;

import javax.enterprise.context.ApplicationScoped;
import javax.swing.*;
import java.util.ArrayList;
import java.util.Collections;

@ApplicationScoped
public class BenchmarkResource {

    /**
     * Method to display the process latency over the time of a stream as a line chart
     * @param latencies The process times for every consumed event
     * @param times The elapsed times since the start of consuming
     * @param eventStream The name of the streaming technology (e.g. Kafka)
     * @return String with statistics like min, max, median etc. for the microbenchmark
     */
    public String displayProcessLatency(ArrayList<Double> latencies, ArrayList<Double> times, String eventStream) {

        // Print statistics like min, max, median etc. and save return String
        String statistics = calculateStatistics((ArrayList<Double>) latencies.clone(), eventStream);

        // Make an array copy of the durations ArrayList with double primitives
        double[] latenciesArray = new double[latencies.size()];
        for (int i = 0; i < latencies.size(); i++) {
            latenciesArray[i] = latencies.get(i);
            if (times.get(i) > 120) {
                break;
            }
        }

        // Make an array copy of the times ArrayList with double primitives
        double[] timesArray = new double[times.size()];
        for (int i = 0; i < times.size(); i++) {
            timesArray[i] = times.get(i);
            if (times.get(i) > 120) {
                break;
            }
        }

        // Create and label 2D plot with process latency and elapsed time
        Plot2DPanel plot = new Plot2DPanel();
        plot.addLinePlot("Prozess-Latenz " + eventStream, timesArray, latenciesArray);
        plot.setAxisLabel(0, "Vergangene Zeit in Sekunden");
        plot.setAxisLabel(1, "Millisekunden");

        // Set bound to x- and y-axis to compare it with other stream process latencies
        plot.setFixedBounds(1, 0, 200);
        plot.setFixedBounds(0, 0, 120);

        // Build JFrame with plot data and show it
        JFrame frame = new JFrame("Prozess-Latenz " + eventStream);
        frame.setSize(600, 400);
        frame.setContentPane(plot);
        frame.setVisible(true);

        // Return statistics string
        return statistics;
    }

    /**
     * Method to calculate and return statistics
     * @param latencies The process times for every consumed event
     * @param eventStream The name of the streaming technology (e.g. Kafka)
     * @return String with statistics like min, max, median etc. for the microbenchmark
     */
    private String calculateStatistics(ArrayList<Double> latencies, String eventStream) {

        // Sort latencies
        Collections.sort(latencies);

        // Calculate average duration
        double averageDuration = latencies.stream().mapToDouble(duration -> duration).average().orElse(0.0);

        // Calculate median duration
        double medianDuration = 0;
        // For even iterations
        if (latencies.size() % 2 == 0) {
            medianDuration = latencies.get((int) ((latencies.size()/2) + latencies.get(latencies.size()/2 - 1) / 2));
            // For odd iterations
        } else {
            medianDuration = latencies.get(latencies.size()/2);
        }

        // Calculate variance and standard deviation
        float sum = 0;

        // Find sum of square distances to the mean for every duration
        for (Double latency : latencies) {
            float squareDiffToMean = (float) Math.pow(latency - averageDuration, 2);
            sum += squareDiffToMean;
        }

        // Divide sum by number of iterations to get variance and take square root
        double standardDeviation = Math.sqrt(sum / (double) latencies.size());

        // Prepare statistics string
        String statistics = eventStream + " Process Latency\n" +
                "Fastest process latency (min):\t" + latencies.get(0) + "ms\n" +
                "Slowest process latency (max):\t" + latencies.get(latencies.size()-1) + "ms\n" +
                "Average latency (avg/mean):\t" + averageDuration + "ms\n" +
                "Median / 50% Percentile:\t" + medianDuration + "ms\n" +
                "Standard deviation:\t\t" + standardDeviation + "ms\n" +
                "25% Percentile:\t\t\t" + percentile(25, latencies) + "ms\n" +
                "75% Percentile:\t\t\t" + percentile(75, latencies) + "ms\n" +
                "90% Percentile:\t\t\t" + percentile(90, latencies) + "ms\n" +
                "95% Percentile:\t\t\t" + percentile(95, latencies) + "ms\n" +
                "99% Percentile:\t\t\t" + percentile(99, latencies) + "ms";

        // Return statistics
        return statistics;
    }

    /**
     * Method to calculate and return the percentile values
     * @param percentile Border for percentile
     * @param items values to calculate
     * @return Percentile value for given border
     */
    public static double percentile(double percentile, ArrayList<Double> items) {

        // Sort items
        Collections.sort(items);
        // Return percentile
        return items.get((int) Math.round(percentile / 100.0 * (items.size() - 1)));
    }
}
