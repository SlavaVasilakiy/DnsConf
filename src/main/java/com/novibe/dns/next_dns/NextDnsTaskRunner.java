package com.novibe.dns.next_dns;

import com.novibe.common.DnsTaskRunner;
import com.novibe.common.data_sources.HostsBlockListsLoader;
import com.novibe.common.data_sources.HostsOverrideListsLoader;
import com.novibe.common.util.EnvParser;
import com.novibe.common.util.Log;
import com.novibe.dns.next_dns.http.dto.request.CreateRewriteDto;
import com.novibe.dns.next_dns.service.NextDnsDenyService;
import com.novibe.dns.next_dns.service.NextDnsRewriteService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static com.novibe.common.config.EnvironmentVariables.BLOCK;
import static com.novibe.common.config.EnvironmentVariables.REDIRECT;

@Service
@RequiredArgsConstructor
public class NextDnsTaskRunner implements DnsTaskRunner {

    private final HostsBlockListsLoader blockListsLoader;
    private final HostsOverrideListsLoader overrideListsLoader;
    private final NextDnsRewriteService nextDnsRewriteService;
    private final NextDnsDenyService nextDnsDenyService;

    private static final int BATCH_SIZE = 50;      // размер порции для API
    private static final long THROTTLE_MS = 4000;  // пауза между успешными запросами

    @Override
    public void run() {
        Log.global("NextDNS");
        Log.common("""
            Script behaviour: old block/redirect settings are about to be updated via provided block/redirect sources.
            If no sources provided, then all NextDNS settings will be removed.
            If provided only one type of sources, related settings will be updated; another type remain untouched.
            NextDNS API rate limiter reset config: 60 seconds after the last request""");

        // ----------------- BLOCK -----------------
        List<String> blockSources = EnvParser.parse(BLOCK);
        if (!blockSources.isEmpty()) {
            Log.step("Obtain block lists from %s sources".formatted(blockSources.size()));
            List<String> blocks = blockListsLoader.fetchWebsites(blockSources);
            Log.step("Prepare denylist");
            List<String> filteredBlocklist = nextDnsDenyService.dropExistingDenys(blocks);
            Log.common("Prepared %s domains to block".formatted(filteredBlocklist.size()));
            Log.step("Save denylist");

            processInBatches(filteredBlocklist, nextDnsDenyService::saveDenyList);
        } else {
            Log.fail("No block sources provided");
        }

        // ----------------- REDIRECT -----------------
        List<String> rewriteSources = EnvParser.parse(REDIRECT);
        if (!rewriteSources.isEmpty()) {
            Log.step("Obtain rewrite lists from %s sources".formatted(rewriteSources.size()));
            List<HostsOverrideListsLoader.BypassRoute> overrides = overrideListsLoader.fetchWebsites(rewriteSources);
            Log.step("Prepare rewrites");

            Map<String, CreateRewriteDto> requests = nextDnsRewriteService.buildNewRewrites(overrides);
            List<CreateRewriteDto> createRewriteDtos = nextDnsRewriteService.cleanupOutdated(requests);
            Log.common("Prepared %s domains to rewrite".formatted(requests.size()));
            Log.step("Save rewrites");

            processInBatches(createRewriteDtos, nextDnsRewriteService::saveRewrites);
        } else {
            Log.fail("No rewrite sources provided");
        }

        // ----------------- REMOVE -----------------
        if (blockSources.isEmpty() && rewriteSources.isEmpty()) {
            Log.step("Remove settings");
            nextDnsDenyService.removeAll();
            nextDnsRewriteService.removeAll();
        }

        Log.global("FINISHED");
    }

    /**
     * Batch processor с retry при 429
     */
    private <T> void processInBatches(List<T> items, BatchSaver<T> saver) {
        for (int i = 0; i < items.size(); i += BATCH_SIZE) {
            List<T> batch = items.subList(i, Math.min(i + BATCH_SIZE, items.size()));
            boolean success = false;
            while (!success) {
                try {
                    saver.save(batch);
                    success = true;
                    Thread.sleep(THROTTLE_MS); // throttle между успешными запросами
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("429")) {
                        Log.step("Rate limit hit, waiting 60 seconds");
                        try {
                            Thread.sleep(60000L);
                        } catch (InterruptedException ignored) {}
                    } else {
                        Log.fail("Failed to save batch: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    /**
     * Functional interface для batch saver
     */
    @FunctionalInterface
    private interface BatchSaver<T> {
        void save(List<T> batch) throws Exception;
    }
}
