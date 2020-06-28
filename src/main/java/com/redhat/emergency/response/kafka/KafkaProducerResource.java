package com.redhat.emergency.response.kafka;

import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/produce")
public class KafkaProducerResource {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerResource.class);

    @Inject
    @Channel("channel")
    Emitter<String> emitter;

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response produce(String record) {
        JsonObject json = new JsonObject(record);
        String key = json.getString("key");
        if (key == null || key.isBlank()) {
            key = UUID.randomUUID().toString();
        }
        JsonObject value = json.getJsonObject("value");
        if (value == null || value.isEmpty()) {
            log.error("Record value cannot be null or empty");
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        log.info("Producing message");
        log.info("   Key: " + key);
        log.info("   Value: " + value);
        emitter.send(toMessage(key, json.getJsonObject("value").toString()));
        return Response.ok().build();
    }

    private org.eclipse.microprofile.reactive.messaging.Message<String> toMessage(String key, String value) {
        return KafkaRecord.of(key, value);
    }

}
