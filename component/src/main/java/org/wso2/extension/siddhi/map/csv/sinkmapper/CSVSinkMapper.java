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

package org.wso2.extension.siddhi.map.csv.sinkmapper;

import org.apache.commons.csv.CSVFormat;
import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.sink.SinkListener;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The CSV output mapper class will convert a Siddhi Event to CSV format.
 */

@Extension(
        name = "csv",
        namespace = "sinkMapper",
        description = "This output mapper extension allows you to convert Siddhi events processed by the WSO2 SP to " +
                "CSV message before publishing them. You can either use custom placeholder to map a custom CSV" +
                " message or use pre-defined CSV format where event conversion takes place without" +
                " extra configurations.",
        parameters = {
                @Parameter(
                        name = "delimiter",
                        description = "This parameter used to separate the output CSV data, when converting " +
                                "a Siddhi event to CSV format,",
                        optional = true, defaultValue = ",",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "header",
                        description = "This parameter specifies whether the CSV messages will be generated with " +
                                "header or not. If this parameter is set to true, message will be generated " +
                                "with header",
                        optional = true, defaultValue = "false",
                        type = {DataType.BOOL}),
                @Parameter(name = "event.grouping.enabled",
                           description =
                                   "If this parameter is set to `true`, events are grouped via a line.separator" +
                                           " when multiple events are received. It is required to specify " +
                                           "a value for the System.lineSeparator() when the value for this parameter" +
                                           " is `true`.",
                           type = {DataType.BOOL},
                           optional = true,
                           defaultValue = "false"),
        },
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv'))\n" +
                                "define stream BarStream (symbol string, price float, volume long);",
                        description = "Above configuration will perform a default CSV output mapping, which will  " +
                                "generate output as follows:\n " +
                                "WSO2,55.6,100<OS supported line separator>" +

                                "If header is true and delimiter is \"-\", then the output will be as follows:\n" +
                                "symbol-price-volume<OS supported line separator>" +
                                "WSO2-55.6-100<OS supported line separator>"
                ),
                @Example(
                        syntax = "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv',header='true'," +
                                "delimiter='-',@payload(symbol='0',price='2',volume='1')))" +
                                "define stream BarStream (symbol string, price float,volume long); ",
                        description = "Above configuration will perform a custom CSV mapping. Here, user can add " +
                                "custom place order in the @payload. The place order indicates that where the " +
                                "attribute name's value will be appear in the output message, The output will be " +
                                "produced output as follows:\n" +
                                "WSO2,100,55.6\r\n" +

                                "If header is true and delimiter is \"-\", then the output will be as follows:\n" +
                                "symbol-price-volume\r\n" +
                                "WSO2-55.6-100<OS supported line separator>" +

                                "If event grouping is enabled, then the output is as follows:\n" +

                                "WSO2-55.6-100<OS supported line separator>\n" +
                                "WSO2-55.6-100<OS supported line separator>\n" +
                                "WSO2-55.6-100<OS supported line separator>\n"
                )
        }
)

public class CSVSinkMapper extends SinkMapper {
    private static final Logger log = Logger.getLogger(CSVSinkMapper.class);
    private static final String HEADER = "header";
    private static final String DELIMITER = "delimiter";
    private static final String OPTION_GROUP_EVENTS = "event.grouping.enabled";
    private static final String DEFAULT_GROUP_EVENTS = "false";
    private static final String DEFAULT_HEADER = "false";
    private static final String DEFAULT_DELIMITER = ",";

    private StreamDefinition streamDefinition;
    private boolean eventGroupEnabled;
    private char delimiter;
    private Boolean header;
    private Object[] headerOfData;
    private Object[] dataOfEvent;
    private AtomicBoolean isValidateEntry;
    private StringWriter stringWriter;
    private boolean isAddHeader = false;

    /**
     * Returns a list of supported dynamic options  by the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    /**
     * The initialization method for {@link SinkMapper}
     *
     * @param streamDefinition          containing stream definition bind to the {@link SinkMapper}
     * @param optionHolder              Option holder containing static and dynamic configuration related
     *                                  to the {@link SinkMapper}
     * @param payloadTemplateBuilderMap Unmapped payload for reference
     * @param configReader              to read the sink related system configuration.
     * @param siddhiAppContext          the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to
     *                                  get siddhi related utilty functions.
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     Map<String, TemplateBuilder> payloadTemplateBuilderMap, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.header = Boolean.parseBoolean(optionHolder.getOrCreateOption(HEADER, DEFAULT_HEADER).getValue());
        this.eventGroupEnabled = Boolean.valueOf(optionHolder.validateAndGetStaticValue(OPTION_GROUP_EVENTS,
                                                                                        DEFAULT_GROUP_EVENTS));
        this.delimiter = optionHolder.getOrCreateOption(DELIMITER, DEFAULT_DELIMITER).getValue().charAt(0);
        headerOfData = new Object[streamDefinition.getAttributeNameArray().length];
        dataOfEvent = new Object[streamDefinition.getAttributeNameArray().length];
        stringWriter = new StringWriter();
        if (header) {
            isAddHeader = true;
        }
        if (payloadTemplateBuilderMap == null) {
            if (header) {
                for (int i = 0; i < streamDefinition.getAttributeNameArray().length; i++) {
                    headerOfData[i] = streamDefinition.getAttributeNameArray()[i];
                }
            }
        } else {
            headerOfData = new Object[payloadTemplateBuilderMap.size()];
            isValidateEntry = new AtomicBoolean();
            isValidateEntry.set(true);
            for (Map.Entry<String, TemplateBuilder> entry : payloadTemplateBuilderMap.entrySet()) {
                for (String attributeName : streamDefinition.getAttributeNameArray()) {
                    if (attributeName.equals(entry.getKey())) {
                        try {
                            headerOfData[Integer.parseInt(String.valueOf(entry.getValue().build(new Event())))] =
                                    entry.getKey();
                        } catch (NumberFormatException e) {
                            isValidateEntry.set(false);
                            throw new SiddhiAppCreationException(
                                    "[ERROR] " + entry.getKey() + "'s value : " + " should be an Integer in the '"
                                            + streamDefinition.getId() + "' of siddhi CSV input mapper.");
                        } catch (AttributeNotExistException e) {
                            log.error("[ERROR] when arranging the attribute order, " + entry.getKey() +
                                              " isn't in the '" + streamDefinition.getId() +
                                              "' of siddhi custom CSV input mapper.");
                        }
                    }
                }
            }
        }
    }

    /**
     * Returns the list of classes which this sink can consume.
     *
     * @return String array of supported classes.
     * array .
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    /**
     * Method to map the events and send them to {@link SinkListener} for publishing
     *
     * @param events                    {@link Event}s that need to be mapped
     * @param optionHolder              Option holder containing static and dynamic options related to the mapper
     * @param payloadTemplateBuilderMap To build the message payload based on the given template
     * @param sinkListener              {@link SinkListener} that will be called with the mapped events
     */
    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        try {
            if (isAddHeader) {
                CSVFormat.DEFAULT
                        .withDelimiter(delimiter)
                        .withRecordSeparator(System.lineSeparator())
                        .withNullString("null")
                        .withQuote('\"')
                        .printRecord(stringWriter, headerOfData);
                sinkListener.publish(stringWriter.toString());
                isAddHeader = false;
                stringWriter.getBuffer().setLength(0);
            }
            if (payloadTemplateBuilderMap != null && isValidateEntry.get()) { //custom mapping
                if (eventGroupEnabled) {
                    for (Event event : events) {
                        for (int i = 0; i < headerOfData.length; i++) {
                            dataOfEvent[i] = event.getData(streamDefinition.getAttributePosition(
                                    headerOfData[i].toString()));
                        }
                        CSVFormat.DEFAULT
                                .withDelimiter(delimiter)
                                .withNullString("null")
                                .withQuote('\"')
                                .withRecordSeparator(System.lineSeparator())
                                .printRecord(stringWriter, dataOfEvent);
                    }
                    sinkListener.publish(stringWriter.toString());
                    stringWriter.getBuffer().setLength(0);
                } else {
                    for (Event event : events) {
                        for (int i = 0; i < headerOfData.length; i++) {
                            dataOfEvent[i] = event.getData(streamDefinition.getAttributePosition(
                                    headerOfData[i].toString()));
                        }
                        CSVFormat.DEFAULT
                                .withDelimiter(delimiter)
                                .withRecordSeparator(System.lineSeparator())
                                .withNullString("null")
                                .withQuote('\"')
                                .printRecord(stringWriter, dataOfEvent);
                        sinkListener.publish(stringWriter.toString());
                        stringWriter.getBuffer().setLength(0);
                    }
                }
            } else if (payloadTemplateBuilderMap == null) {
                if (eventGroupEnabled) {
                    for (Event event : events) {
                        dataOfEvent = event.getData();
                        CSVFormat.DEFAULT
                                .withDelimiter(delimiter)
                                .withNullString("null")
                                .withQuote('\"')
                                .withRecordSeparator(System.lineSeparator())
                                .printRecord(stringWriter, dataOfEvent);
                    }
                    sinkListener.publish(stringWriter.toString());
                    stringWriter.getBuffer().setLength(0);
                } else {
                    for (Event event : events) {
                        dataOfEvent = event.getData();
                        CSVFormat.DEFAULT
                                .withDelimiter(delimiter)
                                .withRecordSeparator(System.lineSeparator())
                                .withNullString("null")
                                .withQuote('\"')
                                .printRecord(stringWriter, dataOfEvent);
                        sinkListener.publish(stringWriter.toString());
                        stringWriter.getBuffer().setLength(0);
                    }
                }
            }
        } catch (IOException e) {
            log.error("[ERROR] Fail to print the data in csv format from Siddhi event in the stream  '"
                              + streamDefinition.getId() + "' of siddhi CSV output mapper.", e);
        }

    }

    /**
     * Method to map the event and send it to {@link SinkListener} for publishing
     *
     * @param event                     {@link Event} that need to be mapped
     * @param optionHolder              Option holder containing static and dynamic options related to the mapper
     * @param payloadTemplateBuilderMap To build the message payload based on the given template
     * @param sinkListener              {@link SinkListener} that will be called with the mapped event
     */
    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        try {
            if (payloadTemplateBuilderMap != null && isValidateEntry.get()) {
                for (int i = 0; i < headerOfData.length; i++) {
                    dataOfEvent[i] = event.getData(streamDefinition.getAttributePosition(
                            headerOfData[i].toString()));
                }
            } else if (payloadTemplateBuilderMap == null) {
                dataOfEvent = event.getData();
            }
            if (isAddHeader) {
                CSVFormat.DEFAULT
                        .withDelimiter(delimiter)
                        .withRecordSeparator(System.lineSeparator())
                        .withNullString("null")
                        .withQuote('\"')
                        .printRecord(stringWriter, headerOfData);
                sinkListener.publish(stringWriter.toString());
                isAddHeader = false;
                stringWriter.getBuffer().setLength(0);
            }
            CSVFormat.DEFAULT
                    .withDelimiter(delimiter)
                    .withRecordSeparator(System.lineSeparator())
                    .withNullString("null")
                    .withQuote('\"')
                    .printRecord(stringWriter, dataOfEvent);
            sinkListener.publish(stringWriter.toString());
            stringWriter.getBuffer().setLength(0);
        } catch (IOException e) {
            log.error(
                    "[ERROR] Fail to print the data in csv format from Siddhi event in the stream  '"
                            + streamDefinition.getId() + "' of siddhi CSV output mapper.", e);
        }
    }
}

