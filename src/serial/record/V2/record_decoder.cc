// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "record_decoder.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <unordered_map>

#include "serial/utils/V2/utils.h"
#include "serial/utils/V2/keyBuf.h"
#include "serial/record/V2/value_header.h"

namespace dingodb {
namespace serialV2 {

// The worker buffer capacity.
constexpr int kBufInitCapacity = 2048;

using CastAndDecodeOrSkipFuncPointer =
    void (*)(BaseSchemaPtr schema, Buf& key_buf, Buf& value_buf,
             std::vector<std::any>& record, int record_index, bool skip,
             dingodb::serialV2::ValueHeader& valueHeader);

template <typename T>
void CastAndDecodeOrSkip(BaseSchemaPtr schema, Buf& key_buf, Buf& value_buf,
                         std::vector<std::any>& record, int record_index,
                         bool is_skip, dingodb::serialV2::ValueHeader& valueHeader) {

  auto getOffset = [&valueHeader, &value_buf](int index) -> int {
      int start = valueHeader.ids_pos;
      int end = valueHeader.offset_pos - 2;

      while (start <= end) {
        int mid = (start/2 + (end/2 - start/2) / 2) * 2;
        int cur_id = value_buf.ReadShort(mid);
        if (cur_id == index) {
          int offsetPos = (mid - valueHeader.ids_pos)/2;
          return value_buf.ReadInt(valueHeader.offset_pos + offsetPos * 4);
        } else if (cur_id < index) {
          start = mid + 2;
        } else {
          end = mid - 2;
        }
      }

      return -1; //should not come here.
  };

  auto dingo_schema = std::dynamic_pointer_cast<DingoSchema<T>>(schema);
  if (is_skip) {
    if (schema->IsKey()) {
      dingo_schema->SkipKey(key_buf);
    }
  } else {
    if (schema->IsKey()) {
      record.at(record_index) = dingo_schema->DecodeKey(key_buf);
    } else {
      if (value_buf.IsEnd()) {
        record.at(record_index) = std::any();
      } else {
        int offset = getOffset(schema->GetIndex());
        if (offset != -1) {
          record.at(record_index) = dingo_schema->DecodeValue(value_buf, offset);
        } else {
          record.at(record_index) = std::any();
        }
      }
    }
  }
}

CastAndDecodeOrSkipFuncPointer cast_and_decode_or_skip_func_ptrs[] = {
    CastAndDecodeOrSkip<bool>,
    CastAndDecodeOrSkip<int32_t>,
    CastAndDecodeOrSkip<float>,
    CastAndDecodeOrSkip<int64_t>,
    CastAndDecodeOrSkip<double>,
    CastAndDecodeOrSkip<std::string>,
    CastAndDecodeOrSkip<DecimalString>,
    CastAndDecodeOrSkip<std::vector<bool>>,
    CastAndDecodeOrSkip<std::vector<int32_t>>,
    CastAndDecodeOrSkip<std::vector<float>>,
    CastAndDecodeOrSkip<std::vector<int64_t>>,
    CastAndDecodeOrSkip<std::vector<double>>,
    CastAndDecodeOrSkip<std::vector<std::string>>,
    CastAndDecodeOrSkip<std::vector<DecimalString>>,
};

RecordDecoderV2::RecordDecoderV2(int schema_version,
                                 const std::vector<BaseSchemaPtr>& schemas,
                                 long common_id)
    : RecordDecoderV2(schema_version, schemas, common_id, IsLE()) {}

RecordDecoderV2::RecordDecoderV2(int schema_version,
                                 const std::vector<BaseSchemaPtr>& schemas,
                                 long common_id, bool le)
    : le_(le),
      schema_version_(schema_version),
      common_id_(common_id),
      schemas_(schemas) {
  FormatSchema(schemas_, le);
  key_buf_ = Buf(kBufInitCapacity, le);
  value_buf_ = Buf(kBufInitCapacity, le);
}

inline bool RecordDecoderV2::CheckPrefix(Buf& buf) const {
  // skip name space
  buf.Skip(1);
  return buf.ReadLong() == common_id_;
}

inline bool RecordDecoderV2::CheckReverseTag(Buf& buf) const {
  int codec_version = buf.ReverseRead() & 0xFF;
  buf.ReverseRead();
  buf.ReverseRead();
  buf.ReverseRead();

  if (codec_version == codec_version_) {
    return true;
  }
  return false;
}

inline bool RecordDecoderV2::CheckReverseTagForKey(Buf& buf) const {
  int codec_version = buf.ReverseRead();
  buf.ReverseRead();
  buf.ReverseRead();
  buf.ReverseRead();

  if (codec_version == codec_version_) {
    return true;
  }
  return false;
}

inline int RecordDecoderV2::GetCodecVersion(Buf& buf) const {
  return buf.ReadInt(buf.Size() - 4);
}

inline bool RecordDecoderV2::CheckSchemaVersion(Buf& buf) const {
  return buf.ReadInt() <= schema_version_;
}

void DecodeOrSkip(BaseSchemaPtr schema, Buf& key_buf, Buf& value_buf,
                  std::vector<std::any>& record, int record_index, bool skip,
                  dingodb::serialV2::ValueHeader& valueHeader) {
  cast_and_decode_or_skip_func_ptrs[static_cast<int>(schema->GetType())](
      schema, key_buf, value_buf, record, record_index, skip, valueHeader);
}

int RecordDecoderV2::Decode(const std::string& key, const std::string& value,
                            std::vector<std::any>& record /*output*/) {
  KeyBuf key_buf(key, this->le_);
  Buf value_buf(value, this->le_);

  if (!CheckPrefix(key_buf) || !CheckReverseTagForKey(key_buf) ||
      !CheckSchemaVersion(value_buf)) {
    return -1;
  }

  ValueHeader value_header(value_buf);
  if (value_header.total_col_cnt != value_header.cnt_null_col) {
    value_buf.SetReadOffset(value_header.data_pos);
  }

  record.resize(schemas_.size());
  for (const auto& bs : schemas_) {
    if (bs) {
      DecodeOrSkip(bs, key_buf, value_buf, record, bs->GetIndex(), false,
                   value_header);
    }
  }

  return 0;
}

int RecordDecoderV2::Decode(std::string&& key, std::string&& value,
                            std::vector<std::any>& record) {
  KeyBuf key_buf(std::move(key), this->le_);
  Buf value_buf(std::move(value), this->le_);

  if (!CheckPrefix(key_buf) || !CheckReverseTagForKey(key_buf) ||
      !CheckSchemaVersion(value_buf)) {
    return -1;
  }

  ValueHeader value_header(value_buf);
  if (value_header.total_col_cnt != value_header.cnt_null_col) {
    // If not means all value fields are null.
    value_buf.SetReadOffset(value_header.data_pos);
  }

  record.resize(schemas_.size());
  for (const auto& bs : schemas_) {
    if (bs) {
      DecodeOrSkip(bs, key_buf, value_buf, record, bs->GetIndex(), false,
                   value_header);
    }
  }

  return 0;
}

int RecordDecoderV2::DecodeKey(const std::string& key,
                               std::vector<std::any>& record /*output*/) {
  KeyBuf key_buf(key, this->le_);
  Buf value_buf(kBufInitCapacity, this->le_);

  if (!CheckPrefix(key_buf) || !CheckReverseTagForKey(key_buf)) {
    return -1;
  }

  std::unordered_map<int, int> id_offset_map;
  int total_col_cnt = 0;
  ValueHeader value_header;

  record.resize(schemas_.size());
  int index = 0;
  for (const auto& bs : schemas_) {
    if (bs && bs->IsKey()) {
      DecodeOrSkip(bs, key_buf, value_buf, record, index, false, value_header);
    }
    index++;
  }

  return 0;
}

int RecordDecoderV2::Decode(const KeyValue& key_value,
                            std::vector<std::any>& record) {
  return Decode(key_value.GetKey(), key_value.GetValue(), record);
}

int RecordDecoderV2::Decode(const std::string& key, const std::string& value,
                            std::unordered_map<int, int>& column_indexes_serial,
                            std::vector<std::any>& record) {
  KeyBuf key_buf(key, this->le_);
  Buf value_buf(value, this->le_);

  if (!CheckPrefix(key_buf) || !CheckReverseTagForKey(key_buf) ||
      !CheckSchemaVersion(value_buf)) {
    return -1;
  }

  ValueHeader value_header(value_buf);
  if (value_header.total_col_cnt != value_header.cnt_null_col) {
    value_buf.SetReadOffset(value_header.data_pos);
  }

  uint32_t size = column_indexes_serial.size();
  record.resize(size);

  uint32_t decode_col_count = 0;
  for (uint32_t i = 0; i < schemas_.size(); ++i) {
    auto& schema = schemas_[i];
    if (schema == nullptr) {
      continue;
    }
    // if (decode_col_count == size) {
    //   break;
    // }

    if(column_indexes_serial.find(decode_col_count) == column_indexes_serial.end()) {
      DecodeOrSkip(schema, key_buf, value_buf, record, -1, true, value_header);
      decode_col_count++;
    } else {
      int result_index = column_indexes_serial[decode_col_count++];
      DecodeOrSkip(schema, key_buf, value_buf, record, result_index,
                   false, value_header);
    }
  }

  return 0;
}

int RecordDecoderV2::Decode(const KeyValue& key_value,
                            std::unordered_map<int, int>& column_indexes_serial,
                            std::vector<std::any>& record) {
  return Decode(key_value.GetKey(), key_value.GetValue(), column_indexes_serial,
                record);
}

}  // namespace serialV2
}  // namespace dingodb
