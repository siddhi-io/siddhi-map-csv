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

package org.wso2.extension.siddhi.map.csv.sourcemapper;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestCaseOfCSVSourceMapper {
    private static final Logger log = Logger.getLogger(TestCaseOfCSVSourceMapper.class);
    private AtomicInteger count = new AtomicInteger();
    private int waitTime = 50;
    private int timeout = 3000;

    @BeforeMethod
    public void init() {
        count.set(0);
    }

    /**
     * Expected input format:
     * WSO2,55.6,100\n
     */
    @Test
    public void testCSVInputDefaultMapping() throws InterruptedException {
        log.info("_________________Test case for csv input mapping with default mapping____________________");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv')) " +
                "define stream FooStream (symbol string, price float, volume int); " +
                "define stream BarStream (symbol string, price float, volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(55.689f, event.getData(1));
                            assertEquals("null", event.getData(0));
                            break;
                        case 2:
                            assertEquals(75.0f, event.getData(1));
                            assertEquals("IBM@#$%^*", event.getData(0));
                            break;
                        case 3:
                            assertEquals(" ", event.getData(1));
                            break;
                        default:
                            fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", "null,55.689,100");
        InMemoryBroker.publish("stock", "IBM@#$%^*,75,null");
        InMemoryBroker.publish("stock", " WSO2,null,10");
        InMemoryBroker.publish("stock", " WSO2,55.6,aa");
        InMemoryBroker.publish("stock", " WSO2,abb,10");
        InMemoryBroker.publish("stock", " WSO2,bb,10.6");
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVInputDefaultMappingMultipleEvent() throws InterruptedException {
        log.info("_____Test case for csv default mapping for multiple events with header and delimiter______");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv', header='true',delimiter='-')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", "symbol-price-volume");
        InMemoryBroker.publish("stock", "WSO2-55.6-100");
        InMemoryBroker.publish("stock", "IBM-75.6-10");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }

    //Custom mapping
    @Test
    public void testCSVInputCustomMapping() throws InterruptedException {
        log.info("___________Test case for csv input mapping with custom mapping using delimeter___________");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv'," +
                "@attributes(symbol='0',price='2',volume='1')))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(100L, event.getData(2));
                            assertEquals(55.6f, event.getData(1));
                            assertEquals("WSO2", event.getData(0));
                            break;
                        case 2:
                            assertEquals(100L, event.getData(2));
                            assertEquals(55.6f, event.getData(1));
                            assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        InMemoryBroker.publish("stock", "WSO2,100,55.6");
        InMemoryBroker.publish("stock", "IBM,100,55.6");
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVInputCustomMapping1() throws InterruptedException {
        log.info("______Test case for csv input mapping with custom mapping using delimeter and header_____");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv',header='true'," +
                "@attributes(symbol=\"2\",price =\"0\",volume =\"1\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(100L, event.getData(2));
                            assertEquals(29.3f, event.getData(1));
                            assertEquals("WSO2,Colombo-3", event.getData(0));
                            break;
                        case 2:
                            assertEquals(243L, event.getData(2));
                            assertEquals(25f, event.getData(1));
                            assertEquals("IBM,Colombo-10", event.getData(0));
                            break;
                        default:
                            fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", "volume,price,symbol");
        InMemoryBroker.publish("stock", "29.3,100,\"WSO2,Colombo-3\"");
        InMemoryBroker.publish("stock", "25,243,\"IBM,Colombo-10\"");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testInputCustomMappingwithfailunknownattribute() throws InterruptedException {
        log.info("____________Test case for csv default mapping with fail.on.unknown.attribute______________");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv',fail.on.unknown.attribute=\"false\"," +
                "@attributes(symbol=\"2\",price =\"0\",volume =\"1\"))) " +
                "define stream FooStream (symbol string, price float, volume int); " +
                "define stream BarStream (symbol string, price float, volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(55.689f, event.getData(1));
                            assertEquals("null", event.getData(0));
                            break;
                        case 2:
                            assertEquals(75.0f, event.getData(1));
                            assertEquals("IBM@#$%^*", event.getData(0));
                            break;
                        case 3:
                            assertEquals(null, event.getData(1));
                            break;
                        case 4:
                            assertEquals(null, event.getData(2));
                            break;
                        case 5:
                            assertEquals(null, event.getData(1));
                            break;
                        case 6:
                            assertEquals(null, event.getData(1));
                            break;

                        default:
                            fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", "55.689,100,null");
        InMemoryBroker.publish("stock", "75, ,IBM@#$%^*");
        InMemoryBroker.publish("stock", " ,10,WSO2");
        InMemoryBroker.publish("stock", " 55.6,aa,WSO2");
        InMemoryBroker.publish("stock", " abb,10,WSO2");
        InMemoryBroker.publish("stock", " bb,10.6,WSO2");
        SiddhiTestHelper.waitForEvents(waitTime, 6, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 6, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperEventGroup() throws InterruptedException {
        log.info("Test for custom mapping for event grouping");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv'," +
                "event.grouping.enabled='true'," +
                "@attributes(symbol=\"0\",price=\"2\",volume=\"1\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                org.wso2.siddhi.core.util.EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(2), 110L);
                            break;
                        case 2:
                            assertEquals(event.getData(2), 120L);
                            break;
                        case 3:
                            assertEquals(event.getData(2), 130L);
                            break;
                        case 4:
                            assertEquals(event.getData(2), 140L);
                            break;
                        case 5:
                            assertEquals(event.getData(2), 150L);
                            break;
                        case 6:
                            assertEquals(event.getData(2), 160L);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "WSO2,110,55.6" + System.lineSeparator() +
                "IBM,120,55.6" + System.lineSeparator() +
                "IFS,130,55.6" + System.lineSeparator();
        String event2 = "WSO2,140,55.6" + System.lineSeparator() +
                "IBM,150,55.6" + System.lineSeparator() +
                "IFS,160,55.6" + System.lineSeparator();
        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 6, count, timeout);
        //assert event count
        assertEquals(count.get(), 6);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = org.wso2.siddhi.core.exception.SiddhiAppCreationException.class)
    public void testInputCustomMappingAttributeSize() throws InterruptedException {
        log.info("____________________Test case for csv custom mapping @attribute size______________________");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv'," +
                "@attributes(price =\"0\",volume =\"1\"))) " +
                "define stream FooStream (symbol string, price float, volume int); " +
                "define stream BarStream (symbol string, price float, volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiManager.shutdown();
    }
}
