package com.slack.kaldb.metadata.search;

import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

public class SearchMetadataTest {
    @Test
    public void testSearchMetadata() {
        final String name = "testSearch";
        final String snapshotName = "testSnapshot";
        final String url = "http://10.10.1.1:9090";

        SearchMetadata searchMetadata = new SearchMetadata(name, snapshotName, url);

        assertThat(searchMetadata.name).isEqualTo(name);
        assertThat(searchMetadata.snapshotName).isEqualTo(snapshotName);
        assertThat(searchMetadata.url).isEqualTo(url);
    }

    @Test
    public void testEqualsAndHashCode() {
        final String name = "testSearch";
        final String snapshotName = "testSnapshot";
        final String url = "http://10.10.1.1:9090";

        SearchMetadata searchMetadata1 = new SearchMetadata(name, snapshotName, url);
        SearchMetadata searchMetadata2 = new SearchMetadata(name + "2", snapshotName, url);

        assertThat(searchMetadata1).isEqualTo(searchMetadata1);
        assertThat(searchMetadata1).isNotEqualTo(searchMetadata2);

        Set<SearchMetadata> set = new HashSet<>();
        set.add(searchMetadata1);
        set.add(searchMetadata2);
        assertThat(set.size()).isEqualTo(2);
        assertThat(set).containsOnly(searchMetadata1, searchMetadata2);
    }

    @Test
    public void testValidSearchMetadata() {
        final String name = "testSearch";
        final String snapshotName = "testSnapshot";
        final String url = "http://10.10.1.1:9090";

        assertThatIllegalStateException().isThrownBy(() -> new SearchMetadata("", snapshotName, url));
        assertThatIllegalStateException().isThrownBy(() -> new SearchMetadata(null, snapshotName,
                url));
        assertThatIllegalStateException().isThrownBy(() -> new SearchMetadata(name, "", url));
        assertThatIllegalStateException().isThrownBy(() -> new SearchMetadata(name, null, url));
        assertThatIllegalStateException().isThrownBy(() -> new SearchMetadata(name, snapshotName,""));
        assertThatIllegalStateException().isThrownBy(() -> new SearchMetadata(name, snapshotName,""));
    }
}
