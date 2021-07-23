package com.slack.kaldb.metadata.search;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.catchThrowable;

public class SearchMetadataSerializerTest {
    private final SearchMetadataSerializer serDe =  new SearchMetadataSerializer();

    @Test
    public void testSearchMetadataSerializer() throws InvalidProtocolBufferException {
        final String name = "testSearch";
        final String snapshotName = "testSnapshot";
        final String url = "http://10.10.1.1:9090";

        SearchMetadata searchMetadata = new SearchMetadata(name, snapshotName, url);

        String serializedSearchMetadata = serDe.toJsonStr(searchMetadata);
        assertThat(serializedSearchMetadata).isNotEmpty();

        SearchMetadata deserializedSearchMetadata = serDe.fromJsonStr(serializedSearchMetadata);
        assertThat(deserializedSearchMetadata).isEqualTo(searchMetadata);

        assertThat(deserializedSearchMetadata.name).isEqualTo(name);
        assertThat(deserializedSearchMetadata.snapshotName).isEqualTo(snapshotName);
        assertThat(deserializedSearchMetadata.url).isEqualTo(url);
    }

    @Test
    public void testInvalidSerializations() {
       Throwable serializeNull = catchThrowable(() -> serDe.toJsonStr(null));
       assertThat(serializeNull).isInstanceOf(IllegalArgumentException.class);

        Throwable deserializeNull = catchThrowable(() -> serDe.fromJsonStr(null));
        assertThat(deserializeNull).isInstanceOf(InvalidProtocolBufferException.class);

        Throwable deserializeEmpty = catchThrowable(() -> serDe.fromJsonStr(""));
        assertThat(deserializeEmpty).isInstanceOf(InvalidProtocolBufferException.class);

        Throwable deserializeCorrupt = catchThrowable(() -> serDe.fromJsonStr("test"));
        assertThat(deserializeCorrupt).isInstanceOf(InvalidProtocolBufferException.class);
    }
}
