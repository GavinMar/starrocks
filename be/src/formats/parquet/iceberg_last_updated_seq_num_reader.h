// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "formats/parquet/column_reader.h"

namespace starrocks::parquet {

// Reader for the Iceberg v3 _last_updated_sequence_number metadata column.
//
// After compaction, the per-row sequence number may be stored as a physical column
// in the Parquet file. This reader wraps a delegate reader that reads from the
// physical column. For rows where the physical column value is null (or when no
// physical column exists), it falls back to the file-level dataSequenceNumber.
//
// When no delegate is provided (physical column not in file), all rows get the
// file-level dataSequenceNumber as a constant.
class IcebergLastUpdatedSeqNumReader final : public ColumnReader {
public:
    // Constructor for when no physical column exists in the file.
    // All rows will get the fallback data_sequence_number.
    explicit IcebergLastUpdatedSeqNumReader(int64_t data_sequence_number)
            : ColumnReader(nullptr), _data_sequence_number(data_sequence_number), _delegate(nullptr) {}

    // Constructor for when a physical column exists in the file.
    // Reads from the delegate; for null values, falls back to data_sequence_number.
    IcebergLastUpdatedSeqNumReader(int64_t data_sequence_number, std::unique_ptr<ColumnReader> delegate)
            : ColumnReader(nullptr), _data_sequence_number(data_sequence_number), _delegate(std::move(delegate)) {}

    ~IcebergLastUpdatedSeqNumReader() override = default;

    Status prepare() override;

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;
    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {}
    void set_need_parse_levels(bool need_parse_levels) override {}

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override;

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override;

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override;

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override;

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override;

private:
    int64_t _data_sequence_number = 0;
    std::unique_ptr<ColumnReader> _delegate;
};

} // namespace starrocks::parquet
