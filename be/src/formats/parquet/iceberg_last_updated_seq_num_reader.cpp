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

#include "formats/parquet/iceberg_last_updated_seq_num_reader.h"

#include "column/nullable_column.h"
#include "types/datum.h"

namespace starrocks::parquet {

Status IcebergLastUpdatedSeqNumReader::prepare() {
    if (_delegate) {
        return _delegate->prepare();
    }
    return Status::OK();
}

Status IcebergLastUpdatedSeqNumReader::read_range(const Range<uint64_t>& range, const Filter* filter,
                                                  ColumnPtr& dst) {
    if (!_delegate) {
        // No physical column in the file — fill with the file-level dataSequenceNumber constant.
        Column* dst_col = dst->as_mutable_raw_ptr();
        if (filter == nullptr) {
            for (uint64_t i = range.begin(); i < range.end(); ++i) {
                dst_col->append_datum(Datum(_data_sequence_number));
            }
        } else {
            DCHECK_EQ(filter->size(), range.span_size());
            for (uint64_t i = range.begin(); i < range.end(); ++i) {
                size_t filter_index = i - range.begin();
                if ((*filter)[filter_index]) {
                    dst_col->append_datum(Datum(_data_sequence_number));
                }
            }
        }
        return Status::OK();
    }

    // Physical column exists — read from it, then replace nulls with fallback.
    auto nullable_col = NullableColumn::create(dst->clone_empty(), NullColumn::create());
    RETURN_IF_ERROR(_delegate->read_range(range, filter, nullable_col));

    Column* dst_col = dst->as_mutable_raw_ptr();
    size_t num_rows = nullable_col->size();
    auto* null_col = nullable_col->null_column().get();
    auto* data_col = nullable_col->data_column().get();

    for (size_t i = 0; i < num_rows; ++i) {
        if (null_col->get_data()[i]) {
            // Null in physical column — use file-level fallback
            dst_col->append_datum(Datum(_data_sequence_number));
        } else {
            dst_col->append_datum(data_col->get(i));
        }
    }
    return Status::OK();
}

Status IcebergLastUpdatedSeqNumReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    dst->as_mutable_raw_ptr()->swap_column(*(src->as_mutable_raw_ptr()));
    return Status::OK();
}

void IcebergLastUpdatedSeqNumReader::collect_column_io_range(
        std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset, ColumnIOTypeFlags types,
        bool active) {
    if (_delegate) {
        _delegate->collect_column_io_range(ranges, end_offset, types, active);
    }
}

void IcebergLastUpdatedSeqNumReader::select_offset_index(const SparseRange<uint64_t>& range,
                                                         const uint64_t rg_first_row) {
    if (_delegate) {
        _delegate->select_offset_index(range, rg_first_row);
    }
}

StatusOr<bool> IcebergLastUpdatedSeqNumReader::row_group_zone_map_filter(
        const std::vector<const ColumnPredicate*>& predicates, CompoundNodeType pred_relation,
        const uint64_t rg_first_row, const uint64_t rg_num_rows) const {
    // Cannot do zone map filtering on this virtual column — delegate if we have a physical column
    if (_delegate) {
        return _delegate->row_group_zone_map_filter(predicates, pred_relation, rg_first_row, rg_num_rows);
    }
    return false;
}

StatusOr<bool> IcebergLastUpdatedSeqNumReader::page_index_zone_map_filter(
        const std::vector<const ColumnPredicate*>& predicates, SparseRange<uint64_t>* row_ranges,
        CompoundNodeType pred_relation, const uint64_t rg_first_row, const uint64_t rg_num_rows) {
    if (_delegate) {
        return _delegate->page_index_zone_map_filter(predicates, row_ranges, pred_relation, rg_first_row, rg_num_rows);
    }
    return false;
}

} // namespace starrocks::parquet
