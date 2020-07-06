Siddhi Map CSV
===================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-csv/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-csv/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-map-csv.svg)](https://github.com/siddhi-io/siddhi-map-csv/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-map-csv.svg)](https://github.com/siddhi-io/siddhi-map-csv/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-map-csv.svg)](https://github.com/siddhi-io/siddhi-map-csv/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-map-csv.svg)](https://github.com/siddhi-io/siddhi-map-csv/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-map-csv extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that converts messages with CSV format to/from Siddhi events.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 2.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.map.csv/siddhi-map-csv/">here</a>.
* Versions 1.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.map.csv/siddhi-map-csv">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-csv/api/2.1.0">2.1.0</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-csv/api/2.1.0/#csv-sink-mapper">csv</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink-mapper">Sink Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This output mapper extension allows you to convert Siddhi events processed by the WSO2 SP to CSV message before publishing them. You can either use custom placeholder to map a custom CSV message or use pre-defined CSV format where event conversion takes place without extra configurations.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-csv/api/2.1.0/#csv-source-mapper">csv</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source-mapper">Source Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This extension is used to convert CSV message to Siddhi event input mapper. You can either receive pre-defined CSV message where event conversion takes place without extra configurations,or receive custom CSV message where a custom place order to map from custom CSV message.</p></p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

