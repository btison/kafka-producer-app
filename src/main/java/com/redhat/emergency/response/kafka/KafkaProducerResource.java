package com.redhat.emergency.response.kafka;

import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.header.Headers;
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
        String type = json.getString("type");
        if (type == null || type.isBlank()) {
            log.error("Record type cannot be null or empty");
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        Map<String,Object> headers = json.getJsonObject("headers", new JsonObject()).getMap();
        StringBuffer sb = new StringBuffer();
        headers.forEach((key1, value1) -> sb.append(key1).append(":").append(value1.toString()).append(", "));
        String hs = sb.toString();
        if (hs.length() > 2) {
            hs = hs.substring(0, hs.length()-2);
        }
        log.info("Producing message");
        log.info("   Key: " + key);
        log.info("   Headers: " + hs);
        log.info("   Value: " + value);
        emitter.send(toMessage(key, type, json.getJsonObject("value").toString(), headers));
        return Response.ok().build();
    }

    private org.eclipse.microprofile.reactive.messaging.Message<String> toMessage(String key, String type, String value, Map<String, Object> headers) {
        KafkaRecord<String, String> record = KafkaRecord.of(key, value);
        Headers h = record.getHeaders();
        headers.keySet().stream().collect(Collectors.toMap(Function.identity(), new Function<String, byte[]>() {
            @Override
            public byte[] apply(String s) {
                Object value = headers.get(s);
                if (value == null) {
                    return new byte[]{};
                }
                if (value instanceof String) {
                    return ((String) value).getBytes();
                }
                if (value instanceof Number) {
                    long l = ((Number) value).longValue();
                    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(l).array();
                }
                return value.toString().getBytes();
            }
        })).forEach(h::add);
        return record.addMetadata(OutgoingCloudEventMetadata.builder().withType(type).withTimestamp(OffsetDateTime.now().toZonedDateTime()).build());
    }

}
