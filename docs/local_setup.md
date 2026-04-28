# Local Development Setup

This document explains how to run the local development environment for the Canadian Climate & Property Risk Data Platform.

## Purpose

The local environment is used to test the core data engineering stack before deploying to Azure.

Local services:

```text
PostgreSQL/PostGIS
|
v
Airflow webserver + scheduler
|
v
Spark master + worker
|
v
dbt container