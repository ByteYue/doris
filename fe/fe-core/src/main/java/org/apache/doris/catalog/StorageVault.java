// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateStorageVaultStmt;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class StorageVault implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(OdbcCatalogResource.class);
    public static final String REFERENCE_SPLIT = "@";
    public static final String INCLUDE_DATABASE_LIST = "include_database_list";
    public static final String EXCLUDE_DATABASE_LIST = "exclude_database_list";
    public static final String LOWER_CASE_META_NAMES = "lower_case_meta_names";
    public static final String META_NAMES_MAPPING = "meta_names_mapping";

    public enum StorageVaultType {
        UNKNOWN,
        S3,
        HDFS;

        public static StorageVaultType fromString(String storageVaultTypeType) {
            for (StorageVaultType type : StorageVaultType.values()) {
                if (type.name().equalsIgnoreCase(storageVaultTypeType)) {
                    return type;
                }
            }
            return UNKNOWN;
        }
    }

    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected StorageVaultType type;
    @SerializedName(value = "id")
    protected String id;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    public StorageVault() {
    }

    public StorageVault(String name, StorageVaultType type) {
        this.name = name;
        this.type = type;
    }

    public static StorageVault fromStmt(CreateStorageVaultStmt stmt) throws DdlException {
        StorageVault storageVault = getStorageVaultInstance(stmt.getStorageVaultType(), stmt.getStorageVaultName());
        storageVault.setProperties(stmt.getProperties());
        return storageVault;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * Get StorageVault instance by StorageVault name and type
     * @param type
     * @param name
     * @return
     * @throws DdlException
     */
    private static StorageVault getStorageVaultInstance(StorageVaultType type, String name) throws DdlException {
        StorageVault vault;
        switch (type) {
            // case S3:
            //     vault = new S3StorageVault(name);
            //     break;
            case HDFS:
                vault = new HdfsStorageVault(name);
                break;
            default:
                throw new DdlException("Unknown StorageVault type: " + type);
        }

        return vault;
    }

    public String getName() {
        return name;
    }

    public StorageVaultType getType() {
        return type;
    }

    /**
     * Modify properties in child resources
     * @param properties
     * @throws DdlException
     */
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        notifyUpdate(properties);
    }

    /**
     * Check properties in child resources
     * @param properties
     * @throws AnalysisException
     */
    public void checkProperties(Map<String, String> properties) throws AnalysisException { }

    protected void replaceIfEffectiveValue(Map<String, String> properties, String key, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            properties.put(key, value);
        }
    }

    /**
     * Set and check the properties in child resources
     */
    protected abstract void setProperties(Map<String, String> properties) throws DdlException;

    public abstract Map<String, String> getCopiedProperties();

    /**
     * Fill BaseProcResult with different properties in child resources
     * ResourceMgr.RESOURCE_PROC_NODE_TITLE_NAMES format:
     * | Name | ResourceType | Key | Value |
     */
    protected abstract void getProcNodeData(BaseProcResult result);

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Resource read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Resource.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }

    @Override
    public Resource clone() {
        Resource copied = DeepCopy.copy(this, Resource.class, FeConstants.meta_version);
        if (copied == null) {
            LOG.warn("failed to clone odbc resource: " + getName());
            return null;
        }
        return copied;
    }

    private void notifyUpdate(Map<String, String> properties) {
    }

    public void applyDefaultProperties() {}
}
