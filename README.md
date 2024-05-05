# IP Cardinality Counter

## Technologies Used

- JDK 21

## Description

This application utilizes the HyperLogLog algorithm, as described in the
[paper](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf).

It reads IP addresses from a file and counts the approximate number of distinct addresses.
Standard error of main.cardinality is ±2%.
## How to Run
To run the application, the user should pass the absolute filepath containing IP addresses as an argument.
