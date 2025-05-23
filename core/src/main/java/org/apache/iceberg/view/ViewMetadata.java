/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.view;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ImmutablesStyle")
@Value.Immutable(builder = false)
@Value.Style(allParameters = true, visibilityString = "PACKAGE")
public interface ViewMetadata extends Serializable {
  Logger LOG = LoggerFactory.getLogger(ViewMetadata.class);
  int SUPPORTED_VIEW_FORMAT_VERSION = 1;
  int DEFAULT_VIEW_FORMAT_VERSION = 1;

  String uuid();

  int formatVersion();

  String location();

  default Integer currentSchemaId() {
    // fail when accessing the current schema if ViewMetadata was created through the
    // ViewMetadataParser with an invalid schema id
    int currentSchemaId = currentVersion().schemaId();
    Preconditions.checkArgument(
        schemasById().containsKey(currentSchemaId),
        "Cannot find current schema with id %s in schemas: %s",
        currentSchemaId,
        schemasById().keySet());

    return currentSchemaId;
  }

  List<Schema> schemas();

  int currentVersionId();

  List<ViewVersion> versions();

  List<ViewHistoryEntry> history();

  Map<String, String> properties();

  List<MetadataUpdate> changes();

  @Nullable
  String metadataFileLocation();

  default ViewVersion version(int versionId) {
    return versionsById().get(versionId);
  }

  default ViewVersion currentVersion() {
    // fail when accessing the current version if ViewMetadata was created through the
    // ViewMetadataParser with an invalid view version id
    Preconditions.checkArgument(
        versionsById().containsKey(currentVersionId()),
        "Cannot find current version %s in view versions: %s",
        currentVersionId(),
        versionsById().keySet());

    return versionsById().get(currentVersionId());
  }

  @Value.Derived
  default Map<Integer, ViewVersion> versionsById() {
    ImmutableMap.Builder<Integer, ViewVersion> builder = ImmutableMap.builder();
    for (ViewVersion version : versions()) {
      builder.put(version.versionId(), version);
    }

    return builder.build();
  }

  @Value.Derived
  default Map<Integer, Schema> schemasById() {
    ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
    for (Schema schema : schemas()) {
      builder.put(schema.schemaId(), schema);
    }

    return builder.build();
  }

  default Schema schema() {
    return schemasById().get(currentSchemaId());
  }

  @Value.Check
  default void check() {
    Preconditions.checkArgument(
        formatVersion() > 0 && formatVersion() <= ViewMetadata.SUPPORTED_VIEW_FORMAT_VERSION,
        "Unsupported format version: %s",
        formatVersion());
  }

  static Builder builder() {
    return new Builder();
  }

  static Builder buildFrom(ViewMetadata base) {
    return new Builder(base);
  }

  class Builder {
    private static final int INITIAL_SCHEMA_ID = 0;
    private static final int LAST_ADDED = -1;
    private final List<ViewVersion> versions;
    private final List<Schema> schemas;
    private final List<ViewHistoryEntry> history;
    private final Map<String, String> properties;
    private final List<MetadataUpdate> changes;
    private int formatVersion = DEFAULT_VIEW_FORMAT_VERSION;
    private int currentVersionId;
    private String location;
    private String uuid;
    private String metadataLocation;

    // internal change tracking
    private Integer lastAddedVersionId = null;
    private Integer lastAddedSchemaId = null;
    private ViewHistoryEntry historyEntry = null;
    private ViewVersion previousViewVersion = null;

    // indexes
    private final Map<Integer, ViewVersion> versionsById;
    private final Map<Integer, Schema> schemasById;

    private Builder() {
      this.versions = Lists.newArrayList();
      this.versionsById = Maps.newHashMap();
      this.schemas = Lists.newArrayList();
      this.schemasById = Maps.newHashMap();
      this.history = Lists.newArrayList();
      this.properties = Maps.newHashMap();
      this.changes = Lists.newArrayList();
      this.uuid = null;
    }

    private Builder(ViewMetadata base) {
      this.versions = Lists.newArrayList(base.versions());
      this.versionsById = Maps.newHashMap(base.versionsById());
      this.schemas = Lists.newArrayList(base.schemas());
      this.schemasById = Maps.newHashMap(base.schemasById());
      this.history = Lists.newArrayList(base.history());
      this.properties = Maps.newHashMap(base.properties());
      this.changes = Lists.newArrayList();
      this.formatVersion = base.formatVersion();
      this.currentVersionId = base.currentVersionId();
      this.location = base.location();
      this.uuid = base.uuid();
      this.metadataLocation = null;
      this.previousViewVersion = base.currentVersion();
    }

    public Builder upgradeFormatVersion(int newFormatVersion) {
      Preconditions.checkArgument(
          newFormatVersion >= formatVersion,
          "Cannot downgrade v%s view to v%s",
          formatVersion,
          newFormatVersion);

      if (formatVersion == newFormatVersion) {
        return this;
      }

      this.formatVersion = newFormatVersion;
      changes.add(new MetadataUpdate.UpgradeFormatVersion(newFormatVersion));
      return this;
    }

    public Builder setLocation(String newLocation) {
      Preconditions.checkArgument(null != newLocation, "Invalid location: null");
      if (null != location && location.equals(newLocation)) {
        return this;
      }

      this.location = newLocation;
      changes.add(new MetadataUpdate.SetLocation(newLocation));
      return this;
    }

    public Builder setMetadataLocation(String newMetadataLocation) {
      this.metadataLocation = newMetadataLocation;
      return this;
    }

    public Builder setCurrentVersionId(int newVersionId) {
      if (newVersionId == LAST_ADDED) {
        ValidationException.check(
            lastAddedVersionId != null,
            "Cannot set last version id: no current version id has been set");
        return setCurrentVersionId(lastAddedVersionId);
      }

      if (currentVersionId == newVersionId) {
        return this;
      }

      ViewVersion version = versionsById.get(newVersionId);
      Preconditions.checkArgument(
          version != null, "Cannot set current version to unknown version: %s", newVersionId);

      this.currentVersionId = newVersionId;

      if (lastAddedVersionId != null && lastAddedVersionId == newVersionId) {
        changes.add(new MetadataUpdate.SetCurrentViewVersion(LAST_ADDED));
      } else {
        changes.add(new MetadataUpdate.SetCurrentViewVersion(newVersionId));
      }

      // Use the timestamp from the view version if it was added in current set of changes.
      // Otherwise, use the current system time. This handles cases where the view version
      // was set as current in the past and is being re-activated.
      boolean versionAddedInThisChange =
          changes(MetadataUpdate.AddViewVersion.class)
              .anyMatch(added -> added.viewVersion().versionId() == newVersionId);

      this.historyEntry =
          ImmutableViewHistoryEntry.builder()
              .timestampMillis(
                  versionAddedInThisChange ? version.timestampMillis() : System.currentTimeMillis())
              .versionId(version.versionId())
              .build();

      return this;
    }

    public Builder setCurrentVersion(ViewVersion version, Schema schema) {
      int newSchemaId = addSchemaInternal(schema);
      ViewVersion newVersion =
          ImmutableViewVersion.builder().from(version).schemaId(newSchemaId).build();
      return setCurrentVersionId(addVersionInternal(newVersion));
    }

    public Builder addVersion(ViewVersion version) {
      addVersionInternal(version);
      return this;
    }

    private int addVersionInternal(ViewVersion newVersion) {
      int newVersionId = reuseOrCreateNewViewVersionId(newVersion);
      ViewVersion version = newVersion;
      if (newVersionId != version.versionId()) {
        version = ImmutableViewVersion.builder().from(version).versionId(newVersionId).build();
      }

      if (versionsById.containsKey(newVersionId)) {
        boolean addedInBuilder =
            changes(MetadataUpdate.AddViewVersion.class)
                .anyMatch(added -> added.viewVersion().versionId() == newVersionId);
        this.lastAddedVersionId = addedInBuilder ? newVersionId : null;
        return newVersionId;
      }

      if (newVersion.schemaId() == LAST_ADDED) {
        ValidationException.check(
            lastAddedSchemaId != null, "Cannot set last added schema: no schema has been added");
        version =
            ImmutableViewVersion.builder().from(newVersion).schemaId(lastAddedSchemaId).build();
      }

      Preconditions.checkArgument(
          schemasById.containsKey(version.schemaId()),
          "Cannot add version with unknown schema: %s",
          version.schemaId());

      Set<String> dialects = Sets.newHashSet();
      for (ViewRepresentation repr : version.representations()) {
        if (repr instanceof SQLViewRepresentation) {
          SQLViewRepresentation sql = (SQLViewRepresentation) repr;
          Preconditions.checkArgument(
              dialects.add(sql.dialect().toLowerCase(Locale.ROOT)),
              "Invalid view version: Cannot add multiple queries for dialect %s",
              sql.dialect().toLowerCase(Locale.ROOT));
        }
      }

      versions.add(version);
      versionsById.put(version.versionId(), version);

      if (null != lastAddedSchemaId && version.schemaId() == lastAddedSchemaId) {
        changes.add(
            new MetadataUpdate.AddViewVersion(
                ImmutableViewVersion.builder().from(version).schemaId(LAST_ADDED).build()));
      } else {
        changes.add(new MetadataUpdate.AddViewVersion(version));
      }

      this.lastAddedVersionId = newVersionId;

      return newVersionId;
    }

    private int reuseOrCreateNewViewVersionId(ViewVersion viewVersion) {
      // if the view version already exists, use its id; otherwise use the highest id + 1
      int newVersionId = viewVersion.versionId();
      for (ViewVersion version : versions) {
        if (sameViewVersion(version, viewVersion)) {
          return version.versionId();
        } else if (version.versionId() >= newVersionId) {
          newVersionId = version.versionId() + 1;
        }
      }

      return newVersionId;
    }

    /**
     * Checks whether the given view versions would behave the same while ignoring the view version
     * id, the creation timestamp, and the operation.
     *
     * @param one the view version to compare
     * @param two the view version to compare
     * @return true if the given view versions would behave the same
     */
    private boolean sameViewVersion(ViewVersion one, ViewVersion two) {
      return Objects.equals(one.summary(), two.summary())
          && Objects.equals(one.representations(), two.representations())
          && Objects.equals(one.defaultCatalog(), two.defaultCatalog())
          && Objects.equals(one.defaultNamespace(), two.defaultNamespace())
          && one.schemaId() == two.schemaId();
    }

    public Builder addSchema(Schema schema) {
      addSchemaInternal(schema);
      return this;
    }

    private int addSchemaInternal(Schema schema) {
      int newSchemaId = reuseOrCreateNewSchemaId(schema);
      if (schemasById.containsKey(newSchemaId)) {
        // this schema existed or was already added in the builder
        return newSchemaId;
      }

      Schema newSchema;
      if (newSchemaId != schema.schemaId()) {
        newSchema = new Schema(newSchemaId, schema.columns(), schema.identifierFieldIds());
      } else {
        newSchema = schema;
      }

      schemas.add(newSchema);
      schemasById.put(newSchema.schemaId(), newSchema);
      changes.add(new MetadataUpdate.AddSchema(newSchema));

      this.lastAddedSchemaId = newSchemaId;

      return newSchemaId;
    }

    private int reuseOrCreateNewSchemaId(Schema newSchema) {
      // if the schema already exists, use its id; otherwise use the highest id + 1
      int newSchemaId = INITIAL_SCHEMA_ID;
      for (Schema schema : schemas) {
        if (schema.sameSchema(newSchema)) {
          return schema.schemaId();
        } else if (schema.schemaId() >= newSchemaId) {
          newSchemaId = schema.schemaId() + 1;
        }
      }

      return newSchemaId;
    }

    public Builder setProperties(Map<String, String> updated) {
      if (updated.isEmpty()) {
        return this;
      }

      properties.putAll(updated);
      changes.add(new MetadataUpdate.SetProperties(updated));
      return this;
    }

    public Builder removeProperties(Set<String> propertiesToRemove) {
      if (propertiesToRemove.isEmpty()) {
        return this;
      }

      propertiesToRemove.forEach(properties::remove);
      changes.add(new MetadataUpdate.RemoveProperties(propertiesToRemove));
      return this;
    }

    public ViewMetadata.Builder assignUUID(String newUUID) {
      Preconditions.checkArgument(newUUID != null, "Cannot set uuid to null");
      Preconditions.checkArgument(uuid == null || newUUID.equals(uuid), "Cannot reassign uuid");

      if (!newUUID.equals(uuid)) {
        this.uuid = newUUID;
        changes.add(new MetadataUpdate.AssignUUID(uuid));
      }

      return this;
    }

    public ViewMetadata build() {
      Preconditions.checkArgument(null != location, "Invalid location: null");
      Preconditions.checkArgument(!versions.isEmpty(), "Invalid view: no versions were added");

      // when associated with a metadata file, metadata must have no changes so that the metadata
      // matches exactly what is in the metadata file, which does not store changes. metadata
      // location with changes is inconsistent.
      Preconditions.checkArgument(
          metadataLocation == null || changes.isEmpty(),
          "Cannot create view metadata with a metadata location and changes");

      if (null != historyEntry) {
        history.add(historyEntry);
      }

      if (null != previousViewVersion
          && !PropertyUtil.propertyAsBoolean(
              properties,
              ViewProperties.REPLACE_DROP_DIALECT_ALLOWED,
              ViewProperties.REPLACE_DROP_DIALECT_ALLOWED_DEFAULT)) {
        checkIfDialectIsDropped(previousViewVersion, versionsById.get(currentVersionId));
      }

      int historySize =
          PropertyUtil.propertyAsInt(
              properties,
              ViewProperties.VERSION_HISTORY_SIZE,
              ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);

      Preconditions.checkArgument(
          historySize > 0,
          "%s must be positive but was %s",
          ViewProperties.VERSION_HISTORY_SIZE,
          historySize);

      // expire old versions, but keep at least the versions added in this builder and the current
      // version
      int numVersions =
          ImmutableSet.builder()
              .addAll(
                  changes(MetadataUpdate.AddViewVersion.class)
                      .map(v -> v.viewVersion().versionId())
                      .collect(Collectors.toSet()))
              .add(currentVersionId)
              .build()
              .size();
      int numVersionsToKeep = Math.max(numVersions, historySize);

      List<ViewVersion> retainedVersions;
      List<ViewHistoryEntry> retainedHistory;
      if (versions.size() > numVersionsToKeep) {
        retainedVersions =
            expireVersions(versionsById, numVersionsToKeep, versionsById.get(currentVersionId));
        Set<Integer> retainedVersionIds =
            retainedVersions.stream().map(ViewVersion::versionId).collect(Collectors.toSet());
        retainedHistory = updateHistory(history, retainedVersionIds);
      } else {
        retainedVersions = versions;
        retainedHistory = history;
      }

      return ImmutableViewMetadata.of(
          null == uuid ? UUID.randomUUID().toString() : uuid,
          formatVersion,
          location,
          schemas,
          currentVersionId,
          retainedVersions,
          retainedHistory,
          properties,
          changes,
          metadataLocation);
    }

    @VisibleForTesting
    static List<ViewVersion> expireVersions(
        Map<Integer, ViewVersion> versionsById, int numVersionsToKeep, ViewVersion currentVersion) {
      // version ids are assigned sequentially. keep the latest versions by ID.
      List<Integer> ids = Lists.newArrayList(versionsById.keySet());
      ids.sort(Comparator.reverseOrder());

      List<ViewVersion> retainedVersions = Lists.newArrayList();
      // always retain the current version
      retainedVersions.add(currentVersion);

      for (int idToKeep : ids.subList(0, numVersionsToKeep)) {
        if (retainedVersions.size() == numVersionsToKeep) {
          break;
        }

        ViewVersion version = versionsById.get(idToKeep);
        if (currentVersion.versionId() != version.versionId()) {
          retainedVersions.add(version);
        }
      }

      return retainedVersions;
    }

    @VisibleForTesting
    static List<ViewHistoryEntry> updateHistory(List<ViewHistoryEntry> history, Set<Integer> ids) {
      List<ViewHistoryEntry> retainedHistory = Lists.newArrayList();
      for (ViewHistoryEntry entry : history) {
        if (ids.contains(entry.versionId())) {
          retainedHistory.add(entry);
        } else {
          // clear history past any unknown version
          retainedHistory.clear();
        }
      }

      return retainedHistory;
    }

    private <U extends MetadataUpdate> Stream<U> changes(Class<U> updateClass) {
      return changes.stream().filter(updateClass::isInstance).map(updateClass::cast);
    }

    private void checkIfDialectIsDropped(ViewVersion previous, ViewVersion current) {
      Set<String> baseDialects = sqlDialectsFor(previous);
      Set<String> updatedDialects = sqlDialectsFor(current);

      Preconditions.checkState(
          updatedDialects.containsAll(baseDialects),
          "Cannot replace view due to loss of view dialects (%s=false):\nPrevious dialects: %s\nNew dialects: %s",
          ViewProperties.REPLACE_DROP_DIALECT_ALLOWED,
          baseDialects,
          updatedDialects);
    }

    private Set<String> sqlDialectsFor(ViewVersion viewVersion) {
      Set<String> dialects = Sets.newHashSet();
      for (ViewRepresentation repr : viewVersion.representations()) {
        if (repr instanceof SQLViewRepresentation) {
          SQLViewRepresentation sql = (SQLViewRepresentation) repr;
          dialects.add(sql.dialect().toLowerCase(Locale.ROOT));
        }
      }

      return dialects;
    }
  }
}
