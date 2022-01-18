package com.slack.kaldb.metadata.recovery;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Instant;
import org.junit.Test;

public class RecoveryTaskMetadataTest {

  @Test
  public void testRecoveryTaskMetadata() {
    String name = "name";
    String partitionId = "partitionId";
    long startOffset = 1;
    long endOffset = 2;
    long createdTimeUtc = 3;

    RecoveryTaskMetadata recoveryTaskMetadata =
        new RecoveryTaskMetadata(name, partitionId, startOffset, endOffset, createdTimeUtc);

    assertThat(recoveryTaskMetadata.name).isEqualTo(name);
    assertThat(recoveryTaskMetadata.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTaskMetadata.startOffset).isEqualTo(startOffset);
    assertThat(recoveryTaskMetadata.endOffset).isEqualTo(endOffset);
    assertThat(recoveryTaskMetadata.createdTimeEpochMsUtc).isEqualTo(createdTimeUtc);
  }

  @Test
  public void testRecoveryTaskMetadataEqualsHashcode() {
    String name = "name";
    String partitionId = "partitionId";
    long startOffset = 1;
    long endOffset = 5;
    long createdTimeUtc = 9;

    RecoveryTaskMetadata recoveryTaskMetadataA =
        new RecoveryTaskMetadata(name, partitionId, startOffset, endOffset, createdTimeUtc);
    RecoveryTaskMetadata recoveryTaskMetadataB =
        new RecoveryTaskMetadata(name, partitionId, startOffset, endOffset, createdTimeUtc);
    RecoveryTaskMetadata recoveryTaskMetadataC =
        new RecoveryTaskMetadata(name, "partitionIdC", startOffset, endOffset, createdTimeUtc);
    RecoveryTaskMetadata recoveryTaskMetadataD =
        new RecoveryTaskMetadata(name, partitionId, 3, endOffset, createdTimeUtc);
    RecoveryTaskMetadata recoveryTaskMetadataE =
        new RecoveryTaskMetadata(name, partitionId, startOffset, 8, createdTimeUtc);

    assertThat(recoveryTaskMetadataA).isEqualTo(recoveryTaskMetadataB);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataC);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataD);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataE);

    assertThat(recoveryTaskMetadataA.hashCode()).isEqualTo(recoveryTaskMetadataB.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataC.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataD.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataE.hashCode());
  }

  @Test
  public void invalidArgumentsShouldThrow() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskMetadata(
                    "name", "partitionId", 0, 0, Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new RecoveryTaskMetadata("name", "", 0, 1, Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new RecoveryTaskMetadata("name", null, 0, 1, Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new RecoveryTaskMetadata("name", "partitionId", 0, 1, 0));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskMetadata(
                    "name",
                    "partitionId",
                    Instant.now().toEpochMilli() + 10,
                    Instant.now().toEpochMilli() - 10,
                    Instant.now().toEpochMilli()));
  }
}
