package com.slack.kaldb.metadata.search;

import static com.google.common.base.Preconditions.checkState;

import com.slack.kaldb.metadata.core.KaldbMetadata;

/** Search metadata contains the metadata needed to perform a search on a snapshot. */
public class SearchMetadata extends KaldbMetadata {
  public final String snapshotName;
  public final String url;

  public SearchMetadata(String name, String snapshotName, String url) {
    super(name);
    checkState(url != null && !url.isEmpty(), "Url shouldn't be empty");
    checkState(snapshotName != null && !snapshotName.isEmpty(), "SnapshotName should not be empty");
    this.snapshotName = snapshotName;
    this.url = url;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SearchMetadata that = (SearchMetadata) o;

    if (!snapshotName.equals(that.snapshotName)) return false;
    return url.equals(that.url);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + snapshotName.hashCode();
    result = 31 * result + url.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "SearchMetadata{"
        + "name='"
        + name
        + '\''
        + ", snapshotName='"
        + snapshotName
        + '\''
        + ", url='"
        + url
        + '\''
        + '}';
  }
}
