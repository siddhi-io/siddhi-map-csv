# siddhi-map-csv

The **siddhi-map-csv extension** is an extension to <a target="_blank" href="https://wso2.github
.io/siddhi">Siddhi</a> that supports mapping incoming events with csv format to a stream at the source, and mapping a stream to csv format events at the sink.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-csv">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-csv/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-csv/issues">Issue tracker</a>


## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-csv/api/1.0.4">1.0.4</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-csv/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.map.csv</groupId>
        <artifactId>siddhi-map-csv</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-map-csv/badge/icon)](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-map-csv/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-csv/api/1.0.4/#csv-sink-mapper">csv</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink-mapper">(Sink Mapper)</a>)*<br><div style="padding-left: 1em;"><p>This output mapper extension allows you to convert Siddhi events processed by the WSO2 SP to CSV message before publishing them. You can either use custom placeholder to map a custom CSV message or use pre-defined CSV format where event conversion takes place without extra configurations.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-csv/api/1.0.4/#csv-source-mapper">csv</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source-mapper">(Source Mapper)</a>)*<br><div style="padding-left: 1em;"><p>This extension is used to convert CSV message to Siddhi event input mapper. You can either receive pre-defined CSV message where event conversion takes place without extra configurations,or receive custom CSV message where a custom place order to map from custom CSV message.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-csv/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-csv/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
