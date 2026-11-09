package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.Optional;

@WebMvcTest(MessagesController.class)
class MessagesControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ConsumedStore store;

    @Test
    void listsAllConsumed() throws Exception {
        when(store.all()).thenReturn(List.of(OrderEvent.create("k", "T", "hello")));
        mockMvc.perform(get("/api/messages"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].payload").value("hello"));
    }

    @Test
    void filtersByKeyParam() throws Exception {
        when(store.byKey("a")).thenReturn(List.of(OrderEvent.create("a", "T", "x")));
        mockMvc.perform(get("/api/messages").param("key", "a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].key").value("a"));
    }

    @Test
    void unknownEventIdReturns404() throws Exception {
        when(store.byId("nope")).thenReturn(Optional.empty());
        mockMvc.perform(get("/api/messages/nope")).andExpect(status().isNotFound());
    }

    @Test
    void listsDeadLetters() throws Exception {
        when(store.deadLetters()).thenReturn(List.of(OrderEvent.create("k", "T", "boom")));
        mockMvc.perform(get("/api/dlt"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].payload").value("boom"));
    }
}
