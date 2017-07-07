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

<!-- MarkdownTOC autolink="true" bracket="round" depth=3 -->

  - [Code Hosting](#code-hosting)
    - [Github](#github)
  - [Code Packaging](#code-packaging)
    - [NPM](#npm)
    - [PyPI](#pypi)
    - [RubyGems](#rubygems)
    - [Packagist](#packagist)
  - [Social Media](#social-media)
    - [Twitter](#twitter)
    - [Facebook](#facebook)
  - [Website Analytics](#website-analytics)
    - [Google Analytics](#google-analytics)
  - [Outputs](#outputs)
    - [Outputs Captured by Google Forms](#outputs-captured-by-google-forms)
  - [Email Campaigns](#email-campaigns)
    - [MailChimp](#mailchimp)
- [Environmental Variables](#environmental-variables)
  - [General](#general)
  - [Github](#github-1)
  - [Twitter](#twitter-1)
  - [Facebook](#facebook-1)
  - [Google credentials for PyPI, Google analytics, and Outputs](#google-credentials-for-pypi-google-analytics-and-outputs)
  - [MailChimp](#mailchimp-1)

<!-- /MarkdownTOC -->


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

### Code Hosting

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


### Code Packaging

#### NPM

The NPM processor collects data from the Node Package Manager (NPM) service where our Node and Javascript projects are hosted for distribution. The processor collects the number of daily `downloads` for each package listed in the `packages` section of the project configuration. What is meant by 'downloads' is discussed in this [blog post](http://blog.npmjs.org/post/92574016600/numeric-precision-matters-how-npm-download-counts).

```yaml
config:
  code-packaging:
    npm:
      packages:
        - 'jsontableschema'
        - 'goodtables'
        - 'tableschema'
```

If no data has previously been collected for a particular package, the NPM processor will request daily data for all days since the beginning of the project.

#### PyPI

The PyPI processor collects data from the Python Package Index (PyPI) where our Python projects are hosted for distribution. The processor collects the number of daily `downloads` for each package listed in the `packages` section of the project configuration.

```yaml
config:
  code-packaging:
    pypi:
      packages:
        - 'jsontableschema'
        - 'goodtables'
        - 'tableschema'        
```

If no data has previously been collected for a particular package, the processor will requests daily data from the start date of PyPI's BigQuery database (2016-01-22).

##### PyPI Configuration

The PyPI processor requires a Google API account with generated credential to make BigQuery queries.

1. **Go** to your [Google Cloud Platform Console](https://console.cloud.google.com)
1. **Pick** or **Create the Project** you want
1. **Use Google API**, **Enable API**, search for **Big Query API**, click **ENABLE**
1. Click **Go To Credentials**, there choose **Service Account** Credentials
1. Click on **Options** symbol for **App Engine default service account**, click **Create Key**
1. Choose Key Type to be **JSON**
1. The downloaded file will have all the credentials you need. Keep them safe, and use them to [populate the environmental variables below](#pypi-1).

#### RubyGems

The RubyGems processor collects ruby gem download data from the rubygems.org API.

- **total_downloads**: collected directly from the API
- **downloads**: daily downloads calculated from yesterday's `total_downloads` value, if present.

```yaml
config:
  code-packaging:
    rubygems:
      gems:
        - "tableschema"
        - "datapackage"
```

No historical download data is collected for RubyGems.

#### Packagist

The Packagist processor collects PHP package daily download data from the packagist.org API.

- **downloads**: daily downloads collected directly from the API.

```yaml
config:
  code-packaging:
    packagist:
      packages:
        - "frictionlessdata/tableschema"
        - "frictionlessdata/datapackage"
```

Note: `packages` defined in the config must include their owner organization in the form `organization_name/package_name`.

Results from the Packagist.org API appear to be a couple of days behind.


### Social Media

#### Twitter

The Twitter processor collects data about each entity listed in the `entities` section. Entities can be one of the following:

- `#hashtag`: a twitter hash tag
- `@account`: an account name
- `url:search-term`: a search term as part of a url

For each entity, the processor collects:

- **mentions**: total of tweets mentioning the entity for the period (day)
- **interactions**: total of 'favorites' and 'retweets' for tweets mentioning the hashtag, or tweets authored by the account, for the period (day).

And additionally, for account entities:
- the current number of **followers**

Url search terms are used to find urls mentioned in tweets. It is best to leave off `http://` prefixes. Urls searches for just the domain will be less specific (will return more results) than for url searches that include a path, e.g.: `url:blog.okfn.org` will return more results than the more specific search, `url:blog.okfn.org/2017/`, which in turn will return more results than `url:blog.okfn.org/2017/06/15/the-final-global-open-data-index-is-now-live/`.

```yaml
config:
  social-media:
    twitter:
      entities:
        - "#frictionlessdata"
        - "#datapackages"
        - "@okfnlabs"
        - "url:frictionlessdata.io"
```


#### Facebook

The Facebook processor collects data about each page listed in the `pages` section. For each page, the processor collects:

- **impressions**: The total number of impressions seen of any content associated with your Page, for the collection period (day). This metric is referred to as **"Page Impressions"** (See [here](https://developers.facebook.com/docs/graph-api/reference/v2.7/insights#page_impressions)).
- **interactions**: The total number of _stories_ created about your Page, for the period (day). Stories include: liking your Page, posting to your Page's Wall, liking, commenting on or sharing one of your Page posts, answering a Question you posted, RSVPing to one of your events, mentioning your Page, photo-tagging your Page or checking in at your Place. This metric is referred to as **"Page Stories"** (See _Page and Post Stories and "People talking about this"_ on [Insights](https://developers.facebook.com/docs/graph-api/reference/v2.7/insight)).
- **mentions**: The total number of times a page was "tagged" for the period (day). This metric is a subset of _interactions_.
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
    - Note: If you already a Facebook App for an existing Measure project, you can reuse it and skip this step
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

### Website Analytics

#### Google Analytics

The Google Analytics processor collects visitor data for specified domain. For each domain the following is collected:

- **visitors**: The total number of visits to the domain each day. In GA terms, this is the number of sessions.
- **unique_visitors**: The total number of unique visitors who made at least one visit to the domain that day. In GA terms, this is the number of users.
- **avg_time_spent**: The average session duration in seconds. In GA terms, this is Avg. Session Duration.

Domains are specified in the project configuration and require the domain `url` and Google Analytics `viewid`.

```yaml
config:
  website-analytics:
    ga:
      domains:
        - url: 'frictionlessdata.io'
          viewid: '120195554'
        - url: 'specs.frictionlessdata.io'
          viewid: '57520245'
```

Each `viewid` can be found within your Google Analytics account. See [this short video for guidance](https://www.youtube.com/watch?v=x1MljgyLeRM).

##### Google Analytics Configuration

The Google Analytics processor requires a Google API account with the **Google Analytics Reporting API** enabled.

1. Enable Google Analytics Reporting API:
    - Go to your [Google Cloud Platform Console](https://console.cloud.google.com/)
    - Pick the project you are using
    - Go to **API Manager/Dashboard**
    - Click on **Enable API**, search for **Google Analytics Reporting API**, click **ENABLE**
1. Give Measure credentials to the websites' analytics you'd like to track:
    - Add the service account email to the list of users that has read permissions in the given analytics' accounts

### Outputs

'Outputs' refers to secondary events and products related to a project, e.g. blog posts, talks given, or tangible uses of our products. These can be either internally produced, or external.

We capture these outputs manually using Google Forms, which writes the results to a Google Spreadsheet.

#### Outputs Captured by Google Forms

The Outputs processor requires a Google API account with generated credentials to read private Google Spreadsheet URLs. This is an explanation of what is collected by the processor:

- **source_id**: The id of the source spreadsheet in the form: `{sheetid}/{gid}`
- **source_type**: The purpose for the Outputs Form, e.g 'internal' or 'external'
- **source_email**: The email address of the person who filled out the form
- **output_title**: The title of the output instance. This could be an article title, talk title, or other short description
- **output_type**: The type of output selected by the user, e.g. 'Talk given', or 'Labs hang out'
- **output_organization**: The organization targeted by the output
- **output_person**: Who create/delivered the output
- **output_link**: Link to the output, if appropriate
- **output_date**: Date the output was published/delivered

1. Make a copy of the Outputs Form template for your project (https://docs.google.com/a/okfn.org/forms/d/e/1FAIpQLSfQuBlwZMnWhGjCv4teAMdsKQ3pgbAi08ZwKBtZLAQFw7LqDg/viewform)
2. Configure the associated spreadsheet destination where captured data will be written to. This can be found within the 'Responses' tab for the form, within the settings dropdown > 'Select response destination'
3. Go to the form's spreadsheet and make a note of the `sheetid` and `gid`, which are part of the spreadsheet URL:
```https://docs.google.com/spreadsheets/d/{sheetid}/edit#gid={gid}```
4. Ensure the spreadsheet can be read by the Google API service account that is being used to authorise requests, either by making the spreadsheet public, or by sharing it with the email associated with the service account (defined in the generated credentials)
5. Configure the Measure project with an entry for the Outputs processor:

```yaml
# sheetid and gid correspond with the parts of the spreadsheet url:
# https://docs.google.com/spreadsheets/d/{sheetid}/edit#gid={gid}

config:
  outputs:
    - sheetid: "{sheetid from above}"
      gid: "{gid from above}"
      type: "external"  # the type of outputs captured here
    - sheetid: "{another sheetid}"
      gid: "{another gid}"
      type: "internal"
```


### Email Campaigns

#### MailChimp

The MailChimp processor collects email list data each day. For each list the following is collected:

- **subscribers**: The current total number of subscribers to the list.
- **subs**: The number of added subscribes that day. Counts both opt-ins, and other additions made by admins.
- **unsubs**: The number of removed subscribers that day. Counts both unsubsribes, and other removals by admins.
- **campaigns_sent**: The number of campaigns sent that day.

The processor will attempt to collect historic data upto the creation date of the list. Complete data is collected for `subs`, `unsubs`, and `campaigns_sent`. Partial historic data is collected for `subscribers`; once for the last day of each month when collecting historic data.

List ids are added to the project config file:

```yaml
config:
  email:
    mailchimp:
      lists:
        - 'my-mailchimp-list-id'
        - 'another-mailchimp-list-id'
```

A MailChimp API key must be defined as an environmental variable. See below for details.

## Environmental Variables

Each installation of Measure requires certain environmental variables to be set.

### General

- `MEASURE_DB_ENGINE`: Location of SQL database as a URL Schema
- `MEASURE_TIMESTAMP_DEFAULT_FORMAT`: datetime format used for `timestamp` value. Currently must be `%Y-%m-%dT%H:%M:%SZ`.

### Github

- `MEASURE_GITHUB_API_BASE_URL`: Github API base url (`https://api.github.com/repos/`)
- `MEASURE_GITHUB_API_TOKEN`: Github API token used for making requests

### Twitter

- `MEASURE_TWITTER_API_CONSUMER_KEY`: Twitter app API consumer key
- `MEASURE_TWITTER_API_CONSUMER_SECRET`: Twitter app API consumer secret

### Facebook
- `MEASURE_FACEBOOK_API_ACCESS_TOKEN_{PAGE NAME IN UPPERCASE}`: The page access token obtained from [How to get a Facebook Page Access Token](#how-to-get-a-facebook-page-access-token).

### Google credentials for PyPI, Google analytics, and Outputs
See the [PyPI Big Query API](#pypi-configuration) instructions above to get the values for these env vars:
- `MEASURE_GOOGLE_API_PROJECT_ID`: {project_id}
- `MEASURE_GOOGLE_API_JWT_AUTH_PROVIDER_X509_CERT_URL`: {auth_provider_x509_cert_url}
- `MEASURE_GOOGLE_API_JWT_AUTH_URI`: {auth_uri}
- `MEASURE_GOOGLE_API_JWT_CLIENT_EMAIL`: {client_email}
- `MEASURE_GOOGLE_API_JWT_CLIENT_ID`: {client_id}
- `MEASURE_GOOGLE_API_JWT_CLIENT_X509_CERT_URL`: {client_x509_cert_url}
- `MEASURE_GOOGLE_API_JWT_PRIVATE_KEY`: {private_key}
- `MEASURE_GOOGLE_API_JWT_PRIVATE_KEY_ID`: {private_key_id}
- `MEASURE_GOOGLE_API_JWT_TOKEN_URI`: {token_uri}
- `MEASURE_GOOGLE_API_JWT_TYPE`: {type}

### MailChimp

- `MEASURE_MAILCHIMP_API_TOKEN`: {mailchimp_api_key} (note: must include the data center code, e.g. `123abc456def-dc1`, where `dc1` is the data center code).
