package io.github.vedmider.kafkademo.producer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(PublishController.class)
class PublishControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, OrderEvent> kafka;

    @Test
    void validRequestPublishesKeyedEventAndReturnsIds() throws Exception {
        when(kafka.send(any(), any(), any())).thenReturn(new CompletableFuture<>());

        mockMvc.perform(post("/api/publish").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"customer-1\",\"type\":\"ORDER_PLACED\",\"payload\":\"2 widgets\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.published").value(true))
                .andExpect(jsonPath("$.key").value("customer-1"))
                .andExpect(jsonPath("$.eventId").isNotEmpty());

        ArgumentCaptor<OrderEvent> value = ArgumentCaptor.forClass(OrderEvent.class);
        verify(kafka).send(eq(Topics.EVENTS), eq("customer-1"), value.capture());
        assertThat(value.getValue().key()).isEqualTo("customer-1");
        assertThat(value.getValue().payload()).isEqualTo("2 widgets");
    }

    @Test
    void nullTypeIsAccepted() throws Exception {
        when(kafka.send(any(), any(), any())).thenReturn(new CompletableFuture<>());

        mockMvc.perform(post("/api/publish").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"k\",\"payload\":\"p\"}"))
                .andExpect(status().isOk());

        ArgumentCaptor<OrderEvent> value = ArgumentCaptor.forClass(OrderEvent.class);
        verify(kafka).send(eq(Topics.EVENTS), eq("k"), value.capture());
        assertThat(value.getValue().type()).isNull();
    }

    @Test
    void blankKeyReturns400AndPublishesNothing() throws Exception {
        mockMvc.perform(post("/api/publish").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"  \",\"payload\":\"p\"}"))
                .andExpect(status().isBadRequest());
        verify(kafka, never()).send(any(), any(), any());
    }

    @Test
    void blankPayloadReturns400AndPublishesNothing() throws Exception {
        mockMvc.perform(post("/api/publish").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"k\",\"payload\":\"\"}"))
                .andExpect(status().isBadRequest());
        verify(kafka, never()).send(any(), any(), any());
    }

    @Test
    void malformedJsonReturns400() throws Exception {
        mockMvc.perform(post("/api/publish").contentType(MediaType.APPLICATION_JSON)
                        .content("{not json"))
                .andExpect(status().isBadRequest());
    }
}
