# API Docs - v2.1.3

!!! Info "Tested Siddhi Core version: *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/">5.1.21</a>*"
    It could also support other Siddhi Core minor versions.

## Sinkmapper

### csv *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink-mapper">(Sink Mapper)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This output mapper extension allows you to convert Siddhi events processed by the WSO2 SP to CSV message before publishing them. You can either use custom placeholder to map a custom CSV message or use pre-defined CSV format where event conversion takes place without extra configurations.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(..., @map(type="csv", delimiter="<STRING>", header="<BOOL>", event.grouping.enabled="<BOOL>")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">delimiter</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter used to separate the output CSV data, when converting a Siddhi event to CSV format,</p></td>
        <td style="vertical-align: top">,</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">header</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter specifies whether the CSV messages will be generated with header or not. If this parameter is set to true, message will be generated with header</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">event.grouping.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to <code>true</code>, events are grouped via a line.separator when multiple events are received. It is required to specify a value for the System.lineSeparator() when the value for this parameter is <code>true</code>.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='inMemory', topic='{{symbol}}', @map(type='csv'))
define stream BarStream (symbol string, price float, volume long);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Above configuration will perform a default CSV output mapping, which will  generate output as follows:<br>&nbsp;WSO2,55.6,100&lt;OS supported line separator&gt;If header is true and delimiter is "-", then the output will be as follows:<br>symbol-price-volume&lt;OS supported line separator&gt;WSO2-55.6-100&lt;OS supported line separator&gt;</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@sink(type='inMemory', topic='{{symbol}}', @map(type='csv',header='true',delimiter='-',@payload(symbol='0',price='2',volume='1')))define stream BarStream (symbol string, price float,volume long); 
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Above configuration will perform a custom CSV mapping. Here, user can add custom place order in the @payload. The place order indicates that where the attribute name's value will be appear in the output message, The output will be produced output as follows:<br>WSO2,100,55.6<br>If header is true and delimiter is "-", then the output will be as follows:<br>symbol-price-volume<br>WSO2-55.6-100&lt;OS supported line separator&gt;If event grouping is enabled, then the output is as follows:<br>WSO2-55.6-100&lt;OS supported line separator&gt;<br>WSO2-55.6-100&lt;OS supported line separator&gt;<br>WSO2-55.6-100&lt;OS supported line separator&gt;<br></p>
<p></p>
## Sourcemapper

### csv *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source-mapper">(Source Mapper)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This extension is used to convert CSV message to Siddhi event input mapper. You can either receive pre-defined CSV message where event conversion takes place without extra configurations,or receive custom CSV message where a custom place order to map from custom CSV message.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(..., @map(type="csv", delimiter="<STRING>", header.present="<BOOL>", fail.on.unknown.attribute="<BOOL>", event.grouping.enabled="<BOOL>")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">delimiter</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">When converting a CSV format message to Siddhi event, this parameter indicatesinput CSV message's data should be split by this parameter </p></td>
        <td style="vertical-align: top">,</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">header.present</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">When converting a CSV format message to Siddhi event, this parameter indicates whether CSV message has header or not. This can either have value true or false.If it's set to <code>false</code> then it indicates that CSV message has't header. </p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">fail.on.unknown.attribute</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter specifies how unknown attributes should be handled. If it's set to <code>true</code> and one or more attributes don't havevalues, then SP will drop that message. If this parameter is set to <code>false</code>, the Stream Processor adds the required attribute's values to such events with a null value and the event is converted to a Siddhi event.</p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">event.grouping.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter specifies whether event grouping is enabled or not. To receive a group of events together and generate multiple events, this parameter must be set to <code>true</code>.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='inMemory', topic='stock', @map(type='csv'))
 define stream FooStream (symbol string, price float, volume int); 
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Above configuration will do a default CSV input mapping. Expected input will look like below:<br>&nbsp;WSO2 ,55.6 , 100OR<br>&nbsp;"WSO2,No10,Palam Groove Rd,Col-03" ,55.6 , 100If header.present is true and delimiter is "-", then the input is as follows:<br>symbol-price-volumeWSO2-55.6-100</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@source(type='inMemory', topic='stock', @map(type='csv',header='true', @attributes(symbol = "2", price = "0", volume = "1")))
define stream FooStream (symbol string, price float, volume long); 
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Above configuration will perform a custom CSV mapping. Here, user can add place order of each attribute in the @attribute. The place order indicates where the attribute name's value has appeared in the input.Expected input will look like below:<br>55.6,100,WSO2<br>OR55.6,100,"WSO2,No10,Palm Groove Rd,Col-03"<br>If header is true and delimiter is "-", then the output is as follows:<br>price-volume-symbol<br>55.6-100-WSO2<br>If group events is enabled then input should be as follows:<br>price-volume-symbol<br>55.6-100-WSO2System.lineSeparator()<br>55.6-100-IBMSystem.lineSeparator()<br>55.6-100-IFSSystem.lineSeparator()<br></p>
<p></p>
