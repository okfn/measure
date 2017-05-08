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


### Social Media

#### Twitter

The Twitter processor collects data about each entity listed in the `entities` section. Entities can be either a twitter hashtag (starting with `#`) or an account name (starting with a `@`). For each entity, the processor collects:

- **mentions**: an accumulated total of tweets mentioning the entity
- **interactions**: an accumulated total of 'favorites' and 'retweets' for tweets mentioning the hashtag, or tweets authored by the account.

And additionally, for account entities:
- the current number of **followers**

```yaml
config:
  social-media:
    twitter:
      entities:
        - "#frictionlessdata"
        - "#datapackages"
        - "@okfnlabs"
```


#### Facebook

The Facebook processor collects data about each page listed in the `pages` section. For each page, the processor collects:

- **impressions**: The total number of impressions seen of any content associated with your Page, since the page was created. This metric is referred to as **"Page Impressions"** (See [here](https://developers.facebook.com/docs/graph-api/reference/v2.7/insights#page_impressions)).
- **interactions**: The total number of _stories_ created about your Page, since it's creation. Stories include: liking your Page, posting to your Page's Wall, liking, commenting on or sharing one of your Page posts, answering a Question you posted, RSVPing to one of your events, mentioning your Page, photo-tagging your Page or checking in at your Place. This metric is referred to as **"Page Stories"** (See _Page and Post Stories and "People talking about this"_ on [Insights](https://developers.facebook.com/docs/graph-api/reference/v2.7/insight)).
- **mentions**: The total number of times a page was "tagged" since it's creation. This metric is a subset of _interactions_.
- **followers**: The total number of Facebook Users who "liked" the page. This metric is referred to as **"page fans"**. (See _Page User Demographics_ on [Insights](https://developers.facebook.com/docs/graph-api/reference/v2.7/insight)

```yaml
config:
  social-media:
    facebook:
      pages:
        - "OKFNetwork"
```

Each page listed in the project config file will require a Facebook Page Access Token to be generated and added to the app's Environmental Variables.

##### How to get a Facebook Page Access Token

1. Get Admin permissions for the Page you wish to track:
    - Go to the Page's **settings** page
    - Choose the pane **Page Roles**
    - Add the User that sets the token as an **Analyst** or above
 
2. Create a Facebook App:
    - Go to [Facebook Developers](https://developers.facebook.com/)
    - On the upper right menu, select **My Apps** select **Add a New App**
    - Fill in the details, app name and email address, and and click **Create App ID**
 
3. Create a Page Access Token:
    - Go to [Facebook API Explorer](https://developers.facebook.com/tools/explorer/)
    - On the top-right, choose the _application_ you created previously
    - Below it, open the **Get Token** dropdown, and choose **Get User Token**
    - In the opened window, check the **read_insights** and **manage_pages** permissions
    - Click on **Get Access Token**, and approve
    - Now open again the **Get Token** dropdown, and choose **Get Page Access Token**
    - In the dialog, give the app the permissions it requires (particularly, _manage pages_)
    - Choose the page you wish to track from the dropdown
    - You now have a short-living access token, and it needs to be extended
 
4. Extend Access Token:
    - Still in the same view of the API Explorer, next to the Access Token that appeared, click on the **blue exclamation mark**
    - You'll see the Token's info. Click on **Open in Access Token Tool**
    - In the window, Click on **Extend Access Token**
    - You can use this token, but note it's expiration date - by then you'll need to either extend it or replace it. Or you can proceed to next step to obtain a permanent page access token.

5. Get Permanent Page Access Token
    - Go to [Graph API Explorer](https://developers.facebook.com/tools/explorer/)
    - Select your **app** in **Application**
    - Paste the long-lived access token into **Access Token**
    - Next to **Access Token**, choose the page you want an access token for. The access token appears as a new string.
    - Click i to see the properties of this access token
    - Click “**Open in Access Token Tool**” button again to open the “**Access Token Debugger**” tool to check the properties

6. Add this token to the Environmental Variables for the Measure application
    - Each page must have its own env var to store its token. e.g. for the OKFNetwork page:
    `MEASURE_FACEBOOK_API_ACCESS_TOKEN_OKFNETWORK='{the OKFNetwork page token obtained above}'`

## Installation

### Environmental Variables

Each installation of Measure requires certain environmental variables to be set.

#### General

- `MEASURE_DB_ENGINE`: Location of SQL database as a URL Schema
- `MEASURE_TIMESTAMP_DEFAULT_FORMAT`: datetime format used for `timestamp` value. Currently must be `%Y-%m-%dT%H:%M:%SZ`.

#### Github

- `MEASURE_GITHUB_API_BASE_URL`: Github API base url (`https://api.github.com/repos/`)
- `MEASURE_GITHUB_API_TOKEN`: Github API token used for making requests

#### Twitter

- `MEASURE_TWITTER_API_CONSUMER_KEY`: Twitter app API consumer key
- `MEASURE_TWITTER_API_CONSUMER_SECRET`: Twitter app API consumer secret

#### Facebook
- `MEASURE_FACEBOOK_API_ACCESS_TOKEN_{PAGE NAME IN UPPERCASE}`: The page access token obtained from [How to get a Facebook Page Access Token](#how-to-get-a-facebook-page-access-token).
