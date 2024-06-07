Here's a README file for your GitHub repository based on the provided project report:

---

# Dynamic Book Review Analysis

This repository contains the project **Dynamic Book Review Analysis**, developed as part of the **Database Technologies (DBT)** course (UE21CS343BB3) at PES University.

## Table of Contents

1. [Introduction](#introduction)
2. [Problem Description](#problem-description)
3. [Solution Architecture](#solution-architecture)
4. [Installation](#installation)
5. [Input Data](#input-data)
6. [Experiments](#experiments)
    - [Streaming Mode](#streaming-mode)
    - [Batch Mode](#batch-mode)
7. [Comparison of Streaming and Batch Modes](#comparison-of-streaming-and-batch-modes)
8. [Conclusion](#conclusion)
9. [References](#references)

## Introduction

This project aims to explore the capabilities of Apache Spark and Kafka in processing streaming data. It involves executing multiple workloads such as Spark SQL queries to perform actions, transformations, or aggregations on input data. The input data considered here is book reviews stored in a MySQL Database. Data is continuously streamed from the MySQL database using Kafka and analyzed using Apache Spark.

## Problem Description

The main challenge addressed in this project is processing the massive volume of data in the book-review dataset and performing various actions, transformations, and aggregations on it. This project involves creating multiple topics using Kafka such as score, awards, and author, and continuously publishing data from the MySQL database to these topics. The objective is to demonstrate how Apache Spark and Kafka can efficiently process and analyze real-time streaming data.

## Solution Architecture

The architecture involves the following components:

- **MySQL Database**: Stores the book review data.
- **Apache Kafka**: Streams data from MySQL to Kafka topics.
- **Apache Spark**: Processes and analyzes the streaming data in real-time and batch modes.

## Installation

To run this project, you need the following software installed:

- **Ubuntu 20.04 or 22.04**
- **Zookeeper**
- **Apache Kafka 3.7.0**
- **Apache Spark Streaming (or PySpark) 3.5.1**
- **MySQL Database 8.0.36**

### Data Preprocessing Tools

- **MySQL**: For storing and retrieving book-review data.
- **Kafka**: For streaming data from MySQL to Kafka topics.

### Streaming Apps/Tools

- **Apache Kafka (3.7.0)**: A distributed streaming platform for building real-time data pipelines and streaming applications.
- **Apache Spark Streaming (3.5.1)**: A real-time processing engine built on top of Apache Spark, allowing for processing of data streams in real-time.
- **PySpark (3.5.1)**: A Python API for Spark.

## Input Data

The dataset is a collection of book reviews sourced from a Reddit post, hosted on Kaggle. The dataset includes fields such as ID, author, username, score, author flair text, and awards received. For this project, we focus on three main topics: score, author, and awards.

## Experiments

### Streaming Mode

In this experiment, real-time streaming of data from MySQL to Kafka is achieved using Kafka Connect. The data is then processed using Spark Streaming to apply various actions, transformations, and aggregations on the incoming data streams.

### Batch Mode

In this experiment, the data is read from a Kafka topic in a streaming fashion using Spark Structured Streaming. The data is parsed using a predefined schema and various transformations and actions are performed.

## Comparison of Streaming and Batch Modes

This section provides a comparison of the results and discusses the efficiency of processing book-review data in both streaming and batch modes.

## Conclusion

The project demonstrates the efficient use of Apache Spark and Kafka for real-time processing and analysis of streaming data. It highlights the advantages and challenges of both streaming and batch processing modes.

## References

Include any references or sources used in the project.

---

### Team Members

- **Jugal Kothari** (PES1UG21CS251)
- **Jyotiraditya J** (PES1UG21CS252)
- **K J Prajwal Rai** (PES1UG21CS254)
- **Pranav Bookanakere** (PES1UG21CS429)

### Supervisor

- **Prof. Raghu B. A.**

---

Feel free to edit or add any additional information as necessary for your project.
