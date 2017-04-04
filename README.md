# Measure

## What

Measure is scripts and conventions to build KPI dashboards for projects.

- A standard library of collectors using [Data Package Pipelines](https://github.com/frictionlessdata/datapackage-pipelines) for automated capture data from a range of sources (Google Analytics, Twitter, MailChimp, etc.)
- A set of Google Forms for manually capturing data points related to projects (Talk at a conference, Someone wrote about the project, a novel use of the tooling, etc.)
- All the data goes into a database, with [Redash](https://redash.io) running over the database.
  - A set of basic dashboard templates for Redash to capture common KPIs
  - Easily create custom dashboards for a project, or across a collection of projects

## Why

We need to be more proactive in collecting useful data on the projects we run, and using this data to measure success and failure.

## Context

We also have an internal proof of concept of Measure collecting data from several different data sources for several different projects. 

The data currently gets written to Google Sheets directly, and visualisation is provided by the visualisation features in Google Sheets. 

We have demonstrated the value of this data collection as part of the project lifecycle for a range of internal and external stakeholders.

The main change here is having a clean, openly available codebase, and using a more suitable database and dashboard builder, as well as adding additional collectors.

Potentially, we'd love to see interest from other non-profits who receive funds to execute on projects, and would like a simple yet systematic way to collect data on what they do.
