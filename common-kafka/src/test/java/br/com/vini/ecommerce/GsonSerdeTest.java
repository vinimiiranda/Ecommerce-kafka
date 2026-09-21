package br.com.vini.ecommerce;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GsonSerdeTest {

    private final GsonSerializer<Order> serializer = new GsonSerializer<>();
    private final GsonDeserializer<Order> deserializer = new GsonDeserializer<>();

    GsonSerdeTest() {
        deserializer.configure(Map.of(GsonDeserializer.TYPE_CONFIG, Order.class.getName()), false);
    }

    @Test
    void roundTripPreservesTheOrder() {
        var order = new Order("id-1", new BigDecimal("4500.50"), "joao@email.com");

        var copy = deserializer.deserialize("topic", serializer.serialize("topic", order));

        assertEquals(order.toString(), copy.toString());
        assertEquals(0, order.getAmount().compareTo(copy.getAmount()));
    }

    @Test
    void usesUtf8RegardlessOfPlatformDefault() {
        var order = new Order("id-2", BigDecimal.TEN, "joão.açúcar@email.com");

        var bytes = serializer.serialize("topic", order);

        assertTrue(new String(bytes, StandardCharsets.UTF_8).contains("joão.açúcar@email.com"));
        assertEquals("joão.açúcar@email.com", deserializer.deserialize("topic", bytes).getEmail());
    }

    @Test
    void nullValuesPassThrough() {
        assertNull(serializer.serialize("topic", null));
        assertNull(deserializer.deserialize("topic", null));
    }

    @Test
    void unknownTypeFailsFastOnConfigure() {
        var broken = new GsonDeserializer<Order>();
        assertThrows(RuntimeException.class,
                () -> broken.configure(Map.of(GsonDeserializer.TYPE_CONFIG, "br.com.vini.DoesNotExist"), false));
    }
}
