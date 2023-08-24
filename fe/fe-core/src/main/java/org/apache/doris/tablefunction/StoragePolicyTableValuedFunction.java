package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StoragePolicyTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "storage policy";

    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("StoragePolicyName", ScalarType.createStringType()),
            new Column("PartitionName", ScalarType.createStringType()),
            new Column("PartitionID", ScalarType.createType(PrimitiveType.INT));

    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    private String storagePolicyName;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

    public StoragePolicyTableValuedFunction(Map<String, String> params) throws
            org.apache.doris.nereids.exceptions.AnalysisException {
        if (params.size() != 1) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("storage policy table-valued-function needs at least one param");
        }
        Optional<Map.Entry<String, String>> opt = params.entrySet().stream()
                    .filter(entry -> entry.getKey().toLowerCase().equals("storage_policy")).findAny();
        if (!opt.isPresent()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("storage policy table-valued-function needs at least one param");
        }
        storagePolicyName = opt.get().getValue();
    }

    @Override
    public String getTableName() {
        return NAME;
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return SCHEMA;
    }

    @Override
    public TMetadataType getMetadataType() {
        return return TMetadataType.STORAGE_POLICY;;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.STORAGE_POLICY);
        TStoragePolicyMetadataParam storagePolicyMetadataParam = new TStoragePolicyMetadataParam();
        storagePolicyMetadataParam.setPolicyName(storagePolicyName);
        metaScanRange.setFrontendsParams(storagePolicyMetadataParam);
        return metaScanRange;
    }
}
