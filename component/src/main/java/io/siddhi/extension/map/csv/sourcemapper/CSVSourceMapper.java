/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.map.csv.sourcemapper;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.MappingFailedException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.source.AttributeMapping;
import io.siddhi.core.stream.input.source.InputEventHandler;
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.core.util.AttributeConverter;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This mapper converts CSV string input to {@link io.siddhi.core.event.ComplexEventChunk}.
 */

@Extension(
        name = "csv",
        namespace = "sourceMapper",
        description = "This extension is used to convert CSV message to Siddhi event input mapper. You can " +
                "either receive pre-defined CSV message where event conversion takes place without extra " +
                "configurations,or receive custom CSV message where a custom place order to map from custom CSV " +
                "message.",
        parameters = {
                @Parameter(
                        name = "delimiter",
                        description = "When converting a CSV format message to Siddhi event, this parameter indicates" +
                                "input CSV message's data should be split by this parameter ",
                        optional = true, defaultValue = ",",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "header.present",
                        description = "When converting a CSV format message to Siddhi event, this parameter indicates" +
                                " whether CSV message has header or not. This can either have value true or false." +
                                "If it's set to `false` then it indicates that CSV message has't header. ",
                        optional = true, defaultValue = "false",
                        type = {DataType.BOOL}),
                @Parameter(
                        name = "fail.on.unknown.attribute",
                        description = "This parameter specifies how unknown attributes should be handled. " +
                                "If it's set to `true` and one or more attributes don't have" +
                                "values, then SP will drop that message. If this parameter is set to `false`, " +
                                "the Stream Processor adds the required attribute's values to such events with a " +
                                "null value and the event is converted to a Siddhi event.",
                        optional = true, defaultValue = "true",
                        type = {DataType.BOOL}),
                @Parameter(name = "event.grouping.enabled",
                        description =
                                "This parameter specifies whether event grouping is enabled or not. To receive a " +
                                        "group of events together and generate multiple events, this parameter " +
                                        "must be set to `true`.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),
        },
        examples = {
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='csv'))\n " +
                                "define stream FooStream (symbol string, price float, volume int); ",
                        description = "Above configuration will do a default CSV input mapping. Expected " +
                                "input will look like below:\n" +
                                " WSO2 ,55.6 , 100" +

                                "OR\n" +

                                " \"WSO2,No10,Palam Groove Rd,Col-03\" ,55.6 , 100" +

                                "If header.present is true and delimiter is \"-\", then the input is as follows:\n" +
                                "symbol-price-volume" +
                                "WSO2-55.6-100"
                ),
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='csv',header='true', " +
                                "@attributes(symbol = \"2\", price = \"0\", volume = \"1\")))\n" +
                                "define stream FooStream (symbol string, price float, volume long); ",
                        description = "Above configuration will perform a custom CSV mapping. Here, " +
                                "user can add place order of each attribute in the @attribute. The place order " +
                                "indicates where the attribute name's value has appeared in the input." +
                                "Expected input will look like below:\n" +
                                "55.6,100,WSO2\r\n" +

                                "OR" +

                                "55.6,100,\"WSO2,No10,Palm Groove Rd,Col-03\"\r\n" +

                                "If header is true and delimiter is \"-\", then the output is as follows:\n" +
                                "price-volume-symbol\r\n" +
                                "55.6-100-WSO2\r\n" +

                                "If group events is enabled then input should be as follows:\n" +
                                "price-volume-symbol\r\n" +
                                "55.6-100-WSO2System.lineSeparator()\n" +
                                "55.6-100-IBMSystem.lineSeparator()\n" +
                                "55.6-100-IFSSystem.lineSeparator()\n"
                ),
        }
)

public class CSVSourceMapper extends SourceMapper {

    private static final Logger log = Logger.getLogger(CSVSourceMapper.class);
    private static final String MAPPING_DELIMETER = "delimiter";
    private static final String MAPPING_HEADER = "header";
    private static final String FAIL_ON_UNKNOWN_ATTRIBUTE = "fail.on.unknown.attribute";
    private static final String OPTION_GROUP_EVENTS = "event.grouping.enabled";
    private static final String DEFAULT_FAIL_ON_UNKNOWN_ATTRIBUTE = "true";
    private static final String DEFAULT_EVENT_GROUP = "false";

    private boolean isCustomMappingEnabled = false;
    private StreamDefinition streamDefinition;
    private boolean failOnUnknownAttribute;
    private Character delimiter;
    private boolean eventGroupEnabled;
    private AttributeConverter attributeConverter = new AttributeConverter();
    private List<Attribute> attributeList;
    private List<AttributeMapping> attributeMappingList;
    private Map<String, Attribute.Type> attributeTypeMap = new HashMap<>();
    private Map<String, Integer> attributePositionMap = new HashMap<>();
    private int pointer;

    /**
     * The initialization method for {@link SourceMapper}.
     *
     * @param streamDefinition     Associated output stream definition
     * @param optionHolder         Option holder containing static configuration related to the {@link SourceMapper}
     * @param attributeMappingList Custom attribute mapping for source-mapping
     * @param configReader         to read the {@link SourceMapper} related system configuration.
     * @param siddhiAppContext     the context of the {@link io.siddhi.query.api.SiddhiApp} used to get siddhi
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     List<AttributeMapping> attributeMappingList, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.attributeList = streamDefinition.getAttributeList();
        this.attributeTypeMap = new HashMap<>(attributeList.size());
        this.attributePositionMap = new HashMap<>(attributeList.size());
        this.delimiter = optionHolder.validateAndGetStaticValue(MAPPING_DELIMETER, ",").charAt(0);
        boolean header = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(MAPPING_HEADER, "false"));
        this.failOnUnknownAttribute = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(
                FAIL_ON_UNKNOWN_ATTRIBUTE, DEFAULT_FAIL_ON_UNKNOWN_ATTRIBUTE));
        this.eventGroupEnabled = Boolean.valueOf(optionHolder.validateAndGetStaticValue(OPTION_GROUP_EVENTS,
                DEFAULT_EVENT_GROUP));
        for (Attribute attribute : attributeList) {
            attributeTypeMap.put(attribute.getName(), attribute.getType());
            attributePositionMap.put(attribute.getName(), streamDefinition.getAttributePosition(attribute.getName()));
        }
        if (attributeMappingList != null && attributeMappingList.size() > 0) { // custom mapping
            isCustomMappingEnabled = true;
            this.attributeMappingList = attributeMappingList;
        }
        if (header) {
            pointer = 0;
        } else {
            pointer = 1;
        }
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return String Array of classes that will be output by the source.
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    /**
     * Method to map the incoming event and as pass that via inputEventHandler to process further.
     *
     * @param eventObject       Incoming event Object based on the supported event class imported by the extensions.
     * @param inputEventHandler Handler to pass the converted Siddhi Event for processing
     * @throws InterruptedException if it does not throw the exception immediately due to streaming
     */
    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler)
            throws InterruptedException, MappingFailedException {
        List<ErroneousEvent> failedEvents = new ArrayList<>(0);
        Event[] result = new io.siddhi.core.event.Event[0];
        try {
            if (eventObject == null) {
                failedEvents.add(new ErroneousEvent(null,
                        "Null object received from the Source to CSVsourceMapper"));
                throw new SiddhiAppRuntimeException("Null object received from the Source to CSVsourceMapper");
            } else if (!(eventObject instanceof String)) {
                failedEvents.add(new ErroneousEvent(null,
                        "Invalid input supported type received. Expected String, but found"
                                + eventObject.getClass().getCanonicalName()));
                throw new SiddhiAppRuntimeException("Invalid input supported type received. Expected String, but found"
                        + eventObject.getClass().getCanonicalName());
            } else {
                if (pointer != 0) {
                    result = convertToEvents(eventObject, failedEvents);
                }
                pointer++;
            }
            if (result.length > 0) {
                inputEventHandler.sendEvents(result);
            }
        } catch (Throwable t) {
            log.error("[Error] when converting the event from CSV message: " + String.valueOf(eventObject) +
                    " to Siddhi Event in the stream " + streamDefinition.getId() +
                    " of siddhi CSV input mapper.", t);
            failedEvents.add(new ErroneousEvent(eventObject, t,
                    "[Error] when converting the event from CSV message: " + eventObject +
                            " to Siddhi Event in the stream " + streamDefinition.getId() +
                            " of siddhi CSV input mapper."));
        }
        if (!failedEvents.isEmpty()) {
            throw new MappingFailedException(failedEvents);
        }
    }

    @Override
    protected boolean allowNullInTransportProperties() {
        return !failOnUnknownAttribute;
    }

    /**
     * Method to convert an Object to an event array
     *
     * @param eventObject Incoming event Object
     * @return Event array
     */
    private Event[] convertToEvents(Object eventObject, List<ErroneousEvent> failedEvents) {
        List<Event> eventList = new ArrayList<>();
        Event event = null;
        try {
            if (eventGroupEnabled) {
                for (CSVRecord record : CSVFormat.DEFAULT.withDelimiter(delimiter).
                        withRecordSeparator(System.lineSeparator()).withQuote('\"')
                        .parse(new StringReader(String.valueOf(eventObject)))) {
                    List<String> dataList = new ArrayList<>();
                    int listPosition = 0;
                    for (String field : record) {
                        dataList.add(listPosition, field);
                        listPosition++;
                    }
                    try {
                        if (isCustomMappingEnabled) {
                            event = convertToCustomEvent(dataList);
                        } else {
                            event = convertToDefaultEvent(dataList);
                        }
                        if (event != null) {
                            eventList.add(event);
                        }
                    } catch (MappingFailedException e) {
                        failedEvents.add(new ErroneousEvent(record, e, e.getMessage()));
                    }
                }
            } else {
                for (CSVRecord record : CSVFormat.DEFAULT.withDelimiter(delimiter)
                        .withQuote('\"').parse(new java.io.StringReader(String.valueOf(eventObject)))) {
                    List<String> dataList = new ArrayList<>();
                    for (String field : record) {
                        dataList.add(field);
                    }
                    try {
                        if (isCustomMappingEnabled) {
                            event = convertToCustomEvent(dataList);
                        } else {
                            event = convertToDefaultEvent(dataList);
                        }
                        if (event != null) {
                            eventList.add(event);
                        }
                    } catch (MappingFailedException e) {
                        failedEvents.add(new ErroneousEvent(record, e, e.getMessage()));
                    }
                }
            }
        } catch (IOException e) {
            log.error("[ERROR] Fail to create the CSV parser of siddhi CSV input mapper: ", e);
        }
        return eventList.toArray(new Event[0]);
    }

    /**
     * Method used to build an event from the String,when occurring default mapping
     *
     * @param eventRecords contain object's data in String[] which is used to build an event
     * @return the constructed {@link Event} object
     */
    private Event convertToDefaultEvent(java.util.List<String> eventRecords) throws MappingFailedException {
        Event event = new Event(this.streamDefinition.getAttributeList().size());
        Object[] data = event.getData();
        Attribute.Type type;
        for (int j = 0; j < attributeList.size(); j++) {
            Attribute attribute = attributeList.get(j);
            String attributeName = attribute.getName();
            if ((type = attributeTypeMap.get(attributeName)) != null) {
                try {
                    Object dataValue = null;
                    if (attribute.getType().equals(Attribute.Type.STRING)) {
                        dataValue = eventRecords.get(j);
                    } else {
                        dataValue = attributeConverter.getPropertyValue(String.valueOf(eventRecords.get(j)), type);
                    }
                    if (dataValue == null && failOnUnknownAttribute) {
                        String errMsg = "Invalid format of event because some required attributes are missing in the "
                                + "event '" + event.toString() + "' when check the event data in "
                                + "the stream '" + streamDefinition.getId()
                                + "' of siddhi CSV input mapper.";
                        log.error(errMsg);
                        throw new MappingFailedException(errMsg);
                    }
                    data[attributePositionMap.get(attributeName)] = dataValue;
                } catch (SiddhiAppRuntimeException | NumberFormatException e) {
                    if (failOnUnknownAttribute) {
                        String errMsg = "Incompatible data format. Because value of " + attributeName +
                                " is" + eventRecords.get(j) + " and attribute type is " + type +
                                " in the stream " + streamDefinition.getId() +
                                " of siddhi csv input mapper.";
                        log.error(errMsg);
                        throw new MappingFailedException(errMsg);
                    }
                }
            } else {
                log.warn("Attribute : " + attributeList.get(j).getName() + "is not found in given" +
                        "stream definition. Hence ignoring this attribute");
                return null;
            }
        }
        return event;
    }

    /**
     * Method used to build an event from the String,when occurring custom mapping
     *
     * @param eventRecords contain object's data in String[] which is used to build an event
     * @return the constructed {@link Event} object
     */
    private Event convertToCustomEvent(java.util.List<String> eventRecords) throws MappingFailedException {
        Event event = new Event(this.streamDefinition.getAttributeList().size());
        Object[] data = event.getData();
        Attribute.Type type;
        int position;
        for (AttributeMapping attributeMapping : attributeMappingList) {
            type = attributeTypeMap.get(attributeMapping.getName());
            position = Integer.parseInt(attributeMapping.getMapping());
            try {
                Object dataValue = null;
                if (type.equals(Attribute.Type.STRING)) {
                    dataValue = eventRecords.get(position);
                } else {
                    dataValue = attributeConverter.getPropertyValue(eventRecords.get(position), type);
                }
                if (dataValue == null && failOnUnknownAttribute) {
                    String errMsg = "Invalid format of event because some required attributes are missing in the "
                            + "event '" + event.toString() + "' when check the event data in "
                            + "the stream '" + streamDefinition.getId()
                            + "' of siddhi CSV input mapper.";
                    log.error(errMsg);
                    throw new MappingFailedException(errMsg);
                }
                data[attributeMapping.getPosition()] = dataValue;
            } catch (SiddhiAppRuntimeException | NumberFormatException | MappingFailedException e) {
                if (failOnUnknownAttribute) {
                    String errMsg = "Incompatible data format. Because value of '" + attributeMapping.getName()
                            + "' is " + eventRecords.get(position) + " and attribute type is " + type
                            + " in the stream " + streamDefinition.getId()
                            + " of siddhi csv input mapper.";
                    log.error(errMsg);
                    throw new MappingFailedException(errMsg);
                }
            }
        }
        return event;
    }
}

