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

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.sink.InMemorySink;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfCSVSinkMapper {

    private static final Logger log = Logger.getLogger(TestCaseOfCSVSinkMapper.class);
    private AtomicInteger wso2Count = new AtomicInteger(0);
    private AtomicInteger ibmCount = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger companyCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        wso2Count.set(0);
        ibmCount.set(0);
    }

    //    from FooStream
    //    select symbol,price,volume
    //    publish inMemory options ("topic", "{{symbol}}")
    //    map csv
    @Test
    public void testCSVSinkmapperDefaultMappingWithSiddhiQL() throws InterruptedException {
        log.info("_______________________Test default csv mapping with SiddhiQL___________________");
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" + "@App:name('TestExecutionPlan')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv'))"
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.645f, 100L});
        stockStream.send(new Object[]{"IBM", 75f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"IBM", null, 57L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 2, ibmCount, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        //AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, ibmCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,55.645,100" + System.lineSeparator(),
                                 onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,75.0,100" + System.lineSeparator(),
                                 onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,57.6,100" + System.lineSeparator(),
                                 onMessageList.get(2).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,null,57" + System.lineSeparator(),
                                 onMessageList.get(3).toString());
        executionPlanRuntime.shutdown();

        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperDefaultMappingWithSiddhiQL1() throws InterruptedException {
        log.info("___________________Test data has quotes with SiddhiQL ______________________");
        List<Object> onMessageList = new ArrayList<>();
        AtomicInteger companyCount = new AtomicInteger(0);
        InMemoryBroker.Subscriber subscriberCompany = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                companyCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "company";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberCompany);

        String streams = "" + "@App:name('TestExecutionPlan')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='company', @map(type='csv')) "
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2,NO10,Palam groove Rd", 55.645f, 100L});
        stockStream.send(new Object[]{"IBM,Colombo 7", 75f, 100L});
        stockStream.send(new Object[]{"WSO2,Colombo 10", 57.6f, 100L});
        stockStream.send(new Object[]{"IBM,Colombo 3", null, 57L});
        Thread.sleep(100);
        SiddhiTestHelper.waitForEvents(waitTime, 4, companyCount, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 4, companyCount.get());

        AssertJUnit.assertEquals("Incorrect mapping!", "\"WSO2,NO10,Palam groove Rd\",55.645,100" +
                        System.lineSeparator(), onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "\"IBM,Colombo 7\",75.0,100" +
                        System.lineSeparator(), onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "\"WSO2,Colombo 10\",57.6,100" +
                        System.lineSeparator(), onMessageList.get(2).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "\"IBM,Colombo 3\",null,57" +
                        System.lineSeparator(), onMessageList.get(3).toString());
        executionPlanRuntime.shutdown();
        InMemoryBroker.unsubscribe(subscriberCompany);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperDefaultMappingWithSiddhiQL2() throws InterruptedException {
        log.info("________________Test default csv mapping with SiddhiQL for events_______________");
        AtomicInteger companyCount = new AtomicInteger(0);
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberCompany = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                companyCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "company";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberCompany);

        String streams = "" + "@App:name('TestExecutionPlan')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='company', @map(type='csv')) "
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        Event wso2Event = new Event(System.currentTimeMillis(), new Object[]{"WSO2#@$", 55.6f, 100L});
        Event ibmEvent = new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Event[]{wso2Event, ibmEvent});
        stockStream.send(new Object[]{null, null, 100L});

        SiddhiTestHelper.waitForEvents(waitTime, 2, companyCount, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 3, companyCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2#@$,55.6,100" + System.lineSeparator(),
                                 onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,75.6,100" + System.lineSeparator(),
                                 onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "null,null,100" + System.lineSeparator()
                , onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberCompany);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperDefaultMappingWithNullElementSiddhiQL() throws InterruptedException {
        log.info("__________________Test default csv mapping with null elements__________________");
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv')) "
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, null});
        stockStream.send(new Object[]{"IBM", null, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,55.6,null" + System.lineSeparator()
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,null,100" + System.lineSeparator()
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,57.6,100" + System.lineSeparator()
                , onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperDefaultMappingWithDelimiterAndHeader() throws InterruptedException {
        log.info("___Here, CSV delimeter and header are being provided for default mapping .____");
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                //wso2Count.incrementAndGet();
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                //ibmCount.incrementAndGet();
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" + "@App:name('TestExecutionPlan')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv', delimiter=\"-\", header='true')) "
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.645f, 100L});
        stockStream.send(new Object[]{"IBM", 75f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"IBM", null, 57L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 2, ibmCount, timeout);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 3, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, ibmCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "symbol-price-volume" + System.lineSeparator()
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2-55.645-100" + System.lineSeparator()
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM-75.0-100" + System.lineSeparator()
                , onMessageList.get(2).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2-57.6-100" + System.lineSeparator()
                , onMessageList.get(3).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM-null-57" + System.lineSeparator()
                , onMessageList.get(4).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVDefaultSinkmapperWithDelimiter() throws InterruptedException {
        log.info("_________________Test default csv mapping with Siddhi Query API________________");
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        org.wso2.siddhi.query.api.definition.StreamDefinition
                streamDefinition = org.wso2.siddhi.query.api.definition.StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT);

        org.wso2.siddhi.query.api.definition.StreamDefinition
                outputDefinition = org.wso2.siddhi.query.api.definition.StreamDefinition.id("BarStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT)
                .annotation(Annotation.annotation("sink")
                                    .element("type", "inMemory")
                                    .element("topic", "{{symbol}}")
                                    .annotation(Annotation.annotation("map")
                                                        .element("type", "csv")
                                                        .element("delimiter", "-")));

        Query query = Query.query();
        query.from(InputStream.stream("FooStream")
                  );
        query.select(Selector
                             .selector().select(new Variable("symbol")).select(new Variable(
                        "price")).select(new Variable("volume"))
                    );
        query.insertInto("BarStream");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiApp executionPlan = new SiddhiApp("ep1");
        executionPlan.defineStream(streamDefinition);
        executionPlan.defineStream(outputDefinition);
        executionPlan.addQuery(query);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(executionPlan);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2-55.6-100" + System.lineSeparator(),
                                 onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM-75.6-100" + System.lineSeparator(),
                                 onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2-57.6-100" + System.lineSeparator(),
                                 onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    //    from FooStream
    //    select symbol,price
    //    publish inMemory options ("topic", "{{symbol}}")
    //    map csv custom

    @Test
    public void testCSVOutputCustomMappingWithoutCSVDelimiter() throws InterruptedException {
        log.info("_____________________Test custom csv mapping________________________________");
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv',"
                + "@payload(symbol='0',price='2',volume='1')))"
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom csv
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,100,55.6" + System.lineSeparator()
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,100,75.6" + System.lineSeparator()
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,100,57.6" + System.lineSeparator()
                , onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperDefaultMappingWithSiddhiQL3() throws InterruptedException {
        log.info("___________________Test custom csv mapping with SiddhiQL for events_______________");
        AtomicInteger companyCount = new AtomicInteger(0);
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberCompany = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                companyCount.incrementAndGet();

            }

            @Override
            public String getTopic() {
                return "company";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberCompany);

        String streams = "" + "@App:name('TestExecutionPlan')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='company', @map(type='csv', "
                + "@payload(symbol=\"0\",price=\"2\",volume=\"1\")))"
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        Event wso2Event = new Event(System.currentTimeMillis(), new Object[]{"WSO2#@$", 55.6f, 100L});
        Event ibmEvent = new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Event[]{wso2Event, ibmEvent});
        stockStream.send(new Object[]{null, null, 100L});

        SiddhiTestHelper.waitForEvents(waitTime, 2, companyCount, timeout);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 3, companyCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2#@$,100,55.6" + System.lineSeparator(),
                                 onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,100,75.6" + System.lineSeparator(),
                                 onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "null,100,null" + System.lineSeparator()
                , onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberCompany);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVOutputCustomMappingWithCSVDelimiter() throws InterruptedException {
        log.info("____________________Test custom csv mapping with delimiter______________________");
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv',delimiter=\"-\","
                + "@payload(symbol='0',price='2',volume='1')))"
                + "define stream BarStream (symbol string, price float, "
                + "volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom csv
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2-100-55.6" + System.lineSeparator()
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM-100-75.6" + System.lineSeparator()
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2-100-57.6" + System.lineSeparator()
                , onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperCustomMappingWithNullAttributes() throws InterruptedException {
        log.info("___________________Test custom csv mapping with null attribute__________________");
        AtomicInteger companyCount = new AtomicInteger(0);
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberCompany = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                companyCount.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "company";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberCompany);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='company', @map(type='csv', "
                + "@payload(symbol=\"0\",price=\"2\",volume=\"1\"))) "
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{null, 56, 100L});
        stockStream.send(new Object[]{"WSO2", null, 100L});
        stockStream.send(new Object[]{"WSO2", 56, null});
        SiddhiTestHelper.waitForEvents(waitTime, 3, companyCount, timeout);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 3, companyCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "null,100,56" + System.lineSeparator(),
                                 onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,100,null" + System.lineSeparator(),
                                 onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,null,56" + System.lineSeparator(),
                                 onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberCompany);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperCustomMappingEventsWithDelimiterAndHeader() throws InterruptedException {
        log.info("___________________Test custom csv mapping with delimiter and header_____________");
        AtomicInteger companyCount = new AtomicInteger(0);
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberCompany = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                companyCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "company";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberCompany);

        String streams = "@App:name('TestExecutionPlan')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='company', @map(type='csv',delimiter=\"-\",header='true',"
                + "@payload(symbol=\"0\",price=\"2\",volume=\"1\")))"
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        Event wso2Event = new Event(System.currentTimeMillis(), new Object[]{"WSO2#@$", 55.6f, 100L});
        Event ibmEvent = new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Event[]{wso2Event, ibmEvent});
        stockStream.send(new Object[]{null, null, 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, companyCount, timeout);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 4, companyCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "symbol-volume-price" + System.lineSeparator(),
                                 onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2#@$-100-55.6" +
                System.lineSeparator(), onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM-100-75.6" + System.lineSeparator(),
                onMessageList.get(2).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "null-100-null" + System.lineSeparator()
                , onMessageList.get(3).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberCompany);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVOutputCustomMappingWithoutCustomattribute() throws InterruptedException {
        log.info("___Test custom csv mapping with SiddhiQL. Here, payload element is missing_______");
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', " + "@map(type='csv')) "
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});

        SiddhiTestHelper.waitForEvents(waitTime, 1, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom csv
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,55.6,100" + System.lineSeparator(),
                                 onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,75.6,100" + System.lineSeparator(),
                                 onMessageList.get(1).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVCustomSinkmapperForEventsWithDelimiterAndHeader() throws InterruptedException {
        log.info("_________________Test custom csv mapping with Siddhi Query API___________________");
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberCompany = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                companyCount.incrementAndGet();

            }

            @Override
            public String getTopic() {
                return "company";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberCompany);

        org.wso2.siddhi.query.api.definition.StreamDefinition
                streamDefinition = StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT);

        org.wso2.siddhi.query.api.definition.StreamDefinition
                outputDefinition = org.wso2.siddhi.query.api.definition.StreamDefinition.id("BarStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT)
                .annotation(Annotation.annotation("sink")
                                    .element("type", "inMemory")
                                    .element("topic", "company")
                                    .annotation(Annotation.annotation("map")
                                                        .element("type", "csv")
                                                        .element("delimiter", "-")
                                                        .element("header", "true")
                                                        .annotation(Annotation.annotation("payload")
                                                                            .element("symbol", "0")
                                                                            .element("price", "2")
                                                                            .element("volume", "1"))));
        Query query = Query.query();
        query.from(
                InputStream.stream("FooStream")
                  );
        query.select(
                Selector
                        .selector().select(new Variable("symbol")).select(new Variable(
                        "price")).select(new Variable("volume"))
                    );
        query.insertInto("BarStream");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiApp executionPlan = new SiddhiApp("ep1");
        executionPlan.defineStream(streamDefinition);
        executionPlan.defineStream(outputDefinition);
        executionPlan.addQuery(query);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(executionPlan);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        Event wso2Event = new Event(System.currentTimeMillis(), new Object[]{"WSO2#@$", 55.6f, 100L});
        Event ibmEvent = new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Event[]{wso2Event, ibmEvent});
        stockStream.send(new Object[]{null, null, 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 4, companyCount, timeout);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 4, companyCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "symbol-volume-price" + System.lineSeparator()
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "null-100-null" + System.lineSeparator(),
                                 onMessageList.get(3).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2#@$-100-55.6" + System.lineSeparator()
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM-100-75.6" + System.lineSeparator(),
                                 onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberCompany);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testCSVOutputCustomMappingWithoutCSVWithDelimiterHeader1() throws InterruptedException {
        log.info("________Test custom CSV mapping ,here check @payload element value type_________");
        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv',"
                + "@payload(symbol='0.5',price='2',volume='1')))"
                + "define stream BarStream (symbol string, price float,volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiManager.shutdown();
    }

    @Test
    public void testTextSinkMapperdafaultEventGroup() throws InterruptedException {
        log.info("____________________Test for default events with grouping enabled_____________________");
        List<Object> onMessageList = new ArrayList<>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv' , event.grouping.enabled='true')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        ArrayList<org.wso2.siddhi.core.event.Event> arrayList = new ArrayList<>(10);
        for (int j = 0; j < 3; j++) {
            arrayList.add(new org.wso2.siddhi.core.event
                    .Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100}));
            arrayList.add(new org.wso2.siddhi.core.event
                    .Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100}));
        }
        stockStream.send(arrayList.toArray(new org.wso2.siddhi.core.event.Event[6]));
        SiddhiTestHelper.waitForEvents(waitTime, 1, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);

        //assert event count
        org.testng.Assert.assertEquals(wso2Count.get(), 1);
        org.testng.Assert.assertEquals(ibmCount.get(), 1);

        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,55.6,100" + System.lineSeparator() +
                                         "WSO2,55.6,100" + System.lineSeparator() +
                                         "WSO2,55.6,100" + System.lineSeparator(),
                                 onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,75.6,100" + System.lineSeparator() +
                                         "IBM,75.6,100" + System.lineSeparator() +
                                         "IBM,75.6,100" + System.lineSeparator(),
                                 onMessageList.get(1).toString());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void testTextcustomSinkMapperGroup() throws InterruptedException {
        log.info("____________________Test for custom events with grouping enabled_____________________");
        List<Object> onMessageList = new ArrayList<>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv' , event.grouping.enabled='true'," +
                "@payload(symbol =\"0\",price=\"2\",volume=\"1\"))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        ArrayList<org.wso2.siddhi.core.event.Event> arrayList = new ArrayList<>(10);
        for (int j = 0; j < 3; j++) {
            arrayList.add(new org.wso2.siddhi.core.event
                    .Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100}));
            arrayList.add(new org.wso2.siddhi.core.event
                    .Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100}));
        }
        stockStream.send(arrayList.toArray(new org.wso2.siddhi.core.event.Event[6]));
        SiddhiTestHelper.waitForEvents(waitTime, 1, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);

        //assert event count
        org.testng.Assert.assertEquals(wso2Count.get(), 1);
        org.testng.Assert.assertEquals(ibmCount.get(), 1);

        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,100,55.6" + System.lineSeparator() +
                                         "WSO2,100,55.6" + System.lineSeparator() +
                                         "WSO2,100,55.6" + System.lineSeparator(),
                                 onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,100,75.6" + System.lineSeparator() +
                                         "IBM,100,75.6" + System.lineSeparator() +
                                         "IBM,100,75.6" + System.lineSeparator(),
                                 onMessageList.get(1).toString());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void testCSVOutputCustomMapping() throws InterruptedException {
        log.info("Test custom csv mapping when all the attributes of stream are not present in the custom mapping");
        List<Object> onMessageList = new ArrayList<>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv',"
                + "@payload(symbol='0',volume='1')))"
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        SiddhiTestHelper.waitForEvents(waitTime, 2, wso2Count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, ibmCount, timeout);
        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom csv
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,100" + System.lineSeparator()
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,100" + System.lineSeparator()
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,100" + System.lineSeparator()
                , onMessageList.get(2).toString());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testCSVOutputCustomMappingWithInvalidIndex() throws InterruptedException {
        log.info("Test custom csv mapping when mapped index is invalid");

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv',"
                + "@payload(symbol='0',volume='2')))"
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" + "from FooStream " + "select * " + "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.start();
        siddhiManager.shutdown();
    }
}
