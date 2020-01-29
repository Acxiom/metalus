# Overview
This project provides the tools necessary for building applications based on reusable components that are assembled at
runtime using external configurations. Using the concepts of *steps*, *pipelines*, *executions* and *applications*, 
developers are able to deploy a single 'uber-jar' (metalus-application) to the cluster and then use the 'spark-submit' 
command as a gateway to assembling the application. Application developers can achieve this by creating reusable *steps* 
that are packaged in a jar and an application JSON configuration or a *DriverSetup* implementation that is responsible 
for parsing the external configuration information to construct an *execution plan* and *pipelines*.

# Table of Contents
* [Introduction](introduction.md)
* [Contributing](contributions.md)
* Core
    * [File Manager](filemanager.md)
    * [Drivers](pipeline-drivers.md)
    * [Audits](executionaudits.md)
* [Steps](steps.md)
    * [Annotations](step-annotations.md)
* Pipelines
    * Mapping
    * Flow Control
        * Branching
        * Fork/Join
        * Execute If Empty
    * Step Groups
    * [JSON Pipelines](json-pipelines.md)
    * [Metadata Extractor](metadata-extractor.md)
    * Chaining
    * Branching
* Applications
    * Globals
    * SparkConf
    * Executions
    * ApplicationDriverSetup
