package com.huberlin.util;

public final class Functions {
    /**
     * Calculates distance between two points in latitude and longitude
     * using Haversine formula.
     * Chatgpt impl of formula from https://en.wikipedia.org/wiki/Haversine_formula#Formulation (d=...)
     *
     * @param lat1 Latitude of the first point in degrees
     * @param lon1 Longitude of the first point in degrees
     * @param lat2 Latitude of the second point in degrees
     * @param lon2 Longitude of the second point in degrees
     * @return Distance between the two points in kilometers
     */
    public static double great_circle_distance(double lat2, double lon2, double lat1, double lon1) {
            // Radius of the Earth in kilometers
            final double EARTH_RADIUS = 6371.0;

            // Convert degrees to radians
            lat1 = Math.toRadians(lat1);
            lon1 = Math.toRadians(lon1);
            lat2 = Math.toRadians(lat2);
            lon2 = Math.toRadians(lon2);

            // Haversine formula
            double dLat = lat2 - lat1;
            double dLon = lon2 - lon1;
            double a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dLon / 2), 2);
            return 2 * EARTH_RADIUS * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)); //Should be arcsin(a^0.5) actually but gpt claims its the same?
    }
}
