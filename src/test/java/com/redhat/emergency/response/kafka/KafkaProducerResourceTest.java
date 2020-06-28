package com.redhat.emergency.response.kafka;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import javax.enterprise.inject.Any;
import javax.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.http.Header;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySink;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Message;
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

        String body = "{\"key\":\"key\",\"value\":{\"id\":\"id\"}}";

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
    }

}
