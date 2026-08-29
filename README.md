# Student Performance Analysis using Hadoop MapReduce

## Project Overview

This project analyzes student academic performance using Apache Hadoop
MapReduce. Student data is stored in HDFS and processed using MapReduce
programs to generate useful performance statistics.

## Technologies Used

- Linux (CachyOS)
- Java 17
- Apache Hadoop 3.4.2
- HDFS
- MapReduce
- Git and GitHub

## Dataset

The project uses a CSV dataset containing 100 student records.

### Input Fields

- ID
- Name
- Department
- Marks
- Attendance

## Project Modules

### 1. Average Marks Analysis

Calculates the average marks of all students using MapReduce.

**Output:**
Average marks of the students.

### 2. Pass/Fail Analysis

Classifies students based on their marks.

- Marks >= 40 → PASS
- Marks < 40 → FAIL

**Output:**
Number of PASS and FAIL students.

### 3. Top Student Analysis

Identifies the student with the highest marks.

**Output:**
Student name and highest marks.

## Hadoop Architecture

Student CSV Dataset
        |
        v
       HDFS
        |
        v
    MapReduce
     /  |  \
    /   |   \
Average Pass/Fail Top Student
 Marks    Count    Analysis
     \    |    /
       HDFS Output

## Input Location

/student-performance/input/students100.csv

## Output Locations

Average Marks:
/student-performance/output

Pass/Fail:
/student-performance/passfail-output

Top Student:
/student-performance/topstudent-output

## MapReduce Tool

The main BDA tool used in this project is Apache Hadoop MapReduce.

## Individual Contribution

Role: MapReduce Developer

Contribution:
- Developed Average Marks MapReduce program
- Developed Pass/Fail MapReduce program
- Developed Top Student MapReduce program
- Stored and processed student data using HDFS
- Tested MapReduce jobs on a 100-student dataset

## How to Run

Compile the Java programs using Hadoop libraries and package them into a JAR file.

Example:

hadoop jar student-performance.jar AverageMarks \
/student-performance/input/students100.csv \
/student-performance/output

Then view the output:

hdfs dfs -cat /student-performance/output/part-r-00000

## Conclusion

The project demonstrates how Hadoop HDFS and MapReduce can be used to
process student performance data and generate meaningful analytical
results from a larger dataset.
