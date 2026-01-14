package com.novibe.dns.next_dns.service;

import com.novibe.common.data_sources.HostsOverrideListsLoader;
import com.novibe.common.util.Log;
import com.novibe.dns.next_dns.http.NextDnsRateLimitedApiProcessor;
import com.novibe.dns.next_dns.http.NextDnsRewriteClient;
import com.novibe.dns.next_dns.http.dto.request.CreateRewriteDto;
import com.novibe.dns.next_dns.http.dto.response.rewrite.RewriteDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;

@Service
@RequiredArgsConstructor
public class NextDnsRewriteService {

    private final NextDnsRewriteClient nextDnsRewriteClient;

    /** Построение новых rewrites из источников */
    public Map<String, CreateRewriteDto> buildNewRewrites(List<HostsOverrideListsLoader.BypassRoute> overrides) {
        Map<String, CreateRewriteDto> rewriteDtos = new HashMap<>();
        overrides.forEach(route ->
                rewriteDtos.putIfAbsent(route.website(), new CreateRewriteDto(route.website(), route.ip())));
        return rewriteDtos;
    }

    /** Удаление устаревших записей и возврат списка актуальных для создания */
    public List<CreateRewriteDto> cleanupOutdated(Map<String, CreateRewriteDto> newRewriteRequests) {
        List<RewriteDto> existingRewrites = getExistingRewrites();
        List<String> outdatedIds = new ArrayList<>();

        for (RewriteDto existingRewrite : existingRewrites) {
            String domain = existingRewrite.name();
            String oldIp = existingRewrite.content();
            CreateRewriteDto request = newRewriteRequests.get(domain);
            if (nonNull(request) && !request.getContent().equals(oldIp)) {
                outdatedIds.add(existingRewrite.id());
            } else {
                newRewriteRequests.remove(domain);
            }
        }

        if (!outdatedIds.isEmpty()) {
            Log.io("Removing %s outdated rewrites from NextDNS".formatted(outdatedIds.size()));
            processElementWise(outdatedIds, nextDnsRewriteClient::deleteRewriteById);
        }

        return List.copyOf(newRewriteRequests.values());
    }

    /** Получение всех существующих rewrites */
    public List<RewriteDto> getExistingRewrites() {
        Log.io("Fetching existing rewrites from NextDNS");
        return nextDnsRewriteClient.fetchRewrites();
    }

    /** Сохранение одного rewrite */
    public void saveRewrite(CreateRewriteDto rewrite) throws Exception {
        processElementWise(List.of(rewrite), nextDnsRewriteClient::saveRewrite);
    }

    /** Удаление всех rewrites */
    public void removeAll() {
        Log.io("Fetching existing rewrites from NextDNS");
        List<RewriteDto> list = nextDnsRewriteClient.fetchRewrites();
        List<String> ids = list.stream().map(RewriteDto::id).toList();
        Log.io("Removing rewrites from NextDNS");
        processElementWise(ids, nextDnsRewriteClient::deleteRewriteById);
    }

    /**
     * Универсальная обработка списка элементов с задержкой и retry при 429
     */
    private <T> void processElementWise(List<T> items, ElementSaver<T> saver) {
        long THROTTLE_MS = 3000; // пауза между отдельными запросами
        for (T item : items) {
            boolean success = false;
            while (!success) {
                try {
                    // лямбда возвращает null для соответствия Function<D,R>
                    NextDnsRateLimitedApiProcessor.callApi(List.of(item), x -> {
                        saver.save(x);
                        return null;
                    });
                    success = true;
                    Thread.sleep(THROTTLE_MS);
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("429")) {
                        Log.step("Rate limit hit, waiting 60 seconds");
                        try { Thread.sleep(60000L); } catch (InterruptedException ignored) {}
                    } else {
                        Log.fail("Failed to save/delete rewrite: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }


    /** Functional interface для обработки одного элемента */
    @FunctionalInterface
    private interface ElementSaver<T> {
        void save(T item) throws Exception;
    }
}
