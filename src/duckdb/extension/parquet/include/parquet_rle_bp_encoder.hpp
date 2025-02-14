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

class RleBpBuffer {
public:
	explicit RleBpBuffer();
	idx_t needs();
	void reserve(idx_t size);
	void push_back(uint32_t elem);
	idx_t size();
	const uint32_t *data();
	void clear();
	bool is_full();

private:
	std::vector<uint32_t> buf;
	idx_t needs_;
	idx_t capacity_;
};

class RleBpEncoder {
public:
	explicit RleBpEncoder(uint32_t bit_width);

public:
	// vectorized API
	template <typename T>
	idx_t GetByteCount(const T *values, idx_t num_values);
	template <typename T>
	void WriteValues(WriteStream &writer, const T *values, idx_t num_values);

	// element-wise API
	void BeginWrite(WriteStream &writer, uint32_t first_value, idx_t num_values);
	void WriteValue(WriteStream &writer, uint32_t value);
	void FinishWrite(WriteStream &writer);

private:
	//! meta information
	uint32_t bit_width;
	uint32_t byte_width;
	uint32_t min_repeat;

	//! RLE run information
	RleBpBuffer bp_buf;
	idx_t current_run_count;
	uint32_t last_value;

private:
	template<typename T>
	void WriteBpRun(WriteStream &writer, const T *values, idx_t count);
	template<typename T>
	void WriteRleRun(WriteStream &writer, T value, idx_t count);

	void WriteBpBuffer(WriteStream &writer);
};

} // namespace duckdb
