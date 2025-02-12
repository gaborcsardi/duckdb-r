//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_rle_bp_encoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parquet_types.h"
#include "thrift_tools.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {

class RleBpEncoder {
public:
	explicit RleBpEncoder(uint32_t bit_width);

public:
	template <typename T>
	idx_t GetByteCount(const T *values, idx_t num_values);
	template <typename T>
	void WriteValues(WriteStream &writer, const T *values, idx_t num_values);
	template <typename T>
	void WriteValues(WriteStream &writer, const T* data, const ValidityMask &mask, idx_t from, idx_t until);

	void BeginWrite(WriteStream &writer, uint32_t first_value);
	void WriteValue(WriteStream &writer, uint32_t value);
	void FinishWrite(WriteStream &writer);

private:
	//! meta information
	uint32_t bit_width;
	uint32_t byte_width;
	uint32_t min_repeat;
	//! RLE run information
	idx_t byte_count;
	idx_t run_count;
	idx_t current_run_count;
	uint32_t last_value;

private:
	void FinishRun();
	void WriteRLERun(WriteStream &writer);
	template<typename T>
	void WriteBPRun(WriteStream &writer, const T *values, idx_t count);

};

} // namespace duckdb
