# Measure

[![Travis](https://img.shields.io/travis/okfn/measure/master.svg)](https://travis-ci.org/okfn/measure)
[![Coveralls](http://img.shields.io/coveralls/okfn/measure.svg?branch=master)](https://coveralls.io/r/okfn/measure?branch=master)

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

## Project Configuration

Each project has a `measure.source-spec.yaml` configuration file within a project directory in `/projects`, e.g. for the Frictionless Data project:

```
/projects/
└── frictionlessdata
    └── measure.source-spec.yaml
└── anotherproject
    └── measure.source-spec.yaml
```

The YAML file defines the project name, and configuration settings for each data source we want to measure. Data sources are grouped by theme, e.g. `code-hosting`, `social-media`, and `code-packaging`. Under each theme is the specific configuration for each data source. Here is an example of the basic structure for a project configuration file:

```yaml
# measure.source-spec.yaml

project: frictionlessdata

config:
  code-hosting: # <------- theme
    github:  # <---------- data source
      repositories:  # <-- data source settings
        - "frictionlessdata/jsontableschema-models-js"
        - "frictionlessdata/datapackage-pipelines"
        [...]

  social-media:
    twitter:
      entities:
        - "#frictionlessdata"
        - "#datapackages"
        [...]
```

Below is the specific configuration settings for each type of data source.

### Code hosting

#### Github

The Github processor collects data about each repository listed in the `repositories` section. For each repository, the processor collects:

- the number of **stars**
- the number of **watchers**

```yaml
config:
  code-hosting:
    github:
      repositories:
        - "frictionlessdata/jsontableschema-models-js"
        - "frictionlessdata/datapackage-pipelines"
```


## Installation

### Environmental Variables

Each installation of Measure requires certain environmental variables to be set.

#### General

- `MEASURE_DB_ENGINE`: Location of SQL database as a URL Schema
- `MEASURE_TIMESTAMP_DEFAULT_FORMAT`: datetime format used for `timestamp` value. Currently must be `%Y-%m-%dT%H:%M:%SZ`.

#### Github

- `MEASURE_GITHUB_API_BASE_URL`: Github api base url (`https://api.github.com/repos/`)
- `MEASURE_GITHUB_API_TOKEN`: Github api token used for making requests
