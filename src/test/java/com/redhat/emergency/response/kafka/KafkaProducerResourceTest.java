package com.redhat.emergency.response.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.enterprise.inject.Any;
import javax.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.http.Header;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecord;
import io.smallrye.reactive.messaging.providers.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.providers.connectors.InMemorySink;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class KafkaProducerResourceTest {

    @Inject
    @Any
    InMemoryConnector connector;

    @BeforeEach
    void init() {
        connector.sink("channel").clear();
    }

    @Test
    void testProduceRecord() {

        InMemorySink<String> results = connector.sink("channel");

        String body = "{\"key\":\"key\", \"headers\":{\"header1\":\"value1\",\"header2\":15}, \"value\":{\"id\":\"id\"}}";

        RestAssured.given().when().with().body(body).header(new Header("Content-Type", "application/json"))
                .post("/produce")
                .then().assertThat().statusCode(200).body(equalTo(""));

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        assertThat(message, instanceOf(OutgoingKafkaRecord.class));
        String value = message.getPayload();
        String key = ((OutgoingKafkaRecord<String, String>)message).getKey();
        assertThat(key, equalTo("key"));
        assertThat(value, equalTo("{\"id\":\"id\"}"));
        Headers headers = ((OutgoingKafkaRecord<String, String>) message).getHeaders();
        assertThat(headers.toArray().length, equalTo(2));
        assertThat(headers.toArray()[0].key(), Matchers.anyOf(equalTo("header1"), equalTo("header2")));
        assertThat(Arrays.stream(headers.toArray()).filter(h -> h.key().equals("header1")).findFirst().orElseThrow().value(), equalTo("value1".getBytes()));
        assertThat(Arrays.stream(headers.toArray()).filter(h -> h.key().equals("header2")).findFirst().orElseThrow().value(), equalTo(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(15L).array()));
    }

    @Test
    void testProduceRecordNoHeaders() {

        InMemorySink<String> results = connector.sink("channel");

        String body = "{\"key\":\"key\", \"value\":{\"id\":\"id\"}}";

        RestAssured.given().when().with().body(body).header(new Header("Content-Type", "application/json"))
                .post("/produce")
                .then().assertThat().statusCode(200).body(equalTo(""));

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        assertThat(message, instanceOf(OutgoingKafkaRecord.class));
        String value = message.getPayload();
        String key = ((OutgoingKafkaRecord<String, String>)message).getKey();
        assertThat(key, equalTo("key"));
        assertThat(value, equalTo("{\"id\":\"id\"}"));
        Headers headers = ((OutgoingKafkaRecord<String, String>) message).getHeaders();
        assertThat(headers.toArray().length, equalTo(0));
    }

}
