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
#include "record_encoder.h"

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common.h"

// #include "common/helper.h"
#include "serial/utils/V2/keyvalue.h"  // IWYU pragma: keep

namespace dingodb {
namespace serialV2 {

// The worker buffer capacity.
constexpr int kBufInitCapacity = 2048;

RecordEncoderV2::RecordEncoderV2(int schema_version,
                                 const std::vector<BaseSchemaPtr>& schemas,
                                 long common_id)
    : RecordEncoderV2(schema_version, schemas, common_id, IsLE()) {}

RecordEncoderV2::RecordEncoderV2(int schema_version,
                                 const std::vector<BaseSchemaPtr>& schemas,
                                 long common_id, bool le)
    : le_(le),
      schema_version_(schema_version),
      common_id_(common_id),
      schemas_(schemas) {
  FormatSchema(schemas_, le);
}

inline void RecordEncoderV2::EncodePrefix(Buf& buf, char prefix) const {
  buf.Write(prefix);
  buf.WriteLong(common_id_);
}

void RecordEncoderV2::EncodeCodecVersion(Buf& buf) const {
  buf.WriteInt(codec_version_);
}

inline void RecordEncoderV2::EncodeSchemaVersion(Buf& buf) const {
  buf.WriteInt(schema_version_);
}

int RecordEncoderV2::Encode(char prefix, const std::vector<std::any>& record,
                            std::string& key, std::string& value) {
  int ret = EncodeKey(prefix, record, key);
  if (ret < 0) {
    return ret;
  }
  ret = EncodeValue(record, value);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

int RecordEncoderV2::EncodeKey(char prefix, const std::vector<std::any>& record,
                               std::string& output) {
  Buf buf(kBufInitCapacity, this->le_);

  // namespace | common_id | ... | codecVersion
  EncodePrefix(buf, prefix);

  // loop meta schemas.
  for (int i = 0; i < schemas_.size(); ++i) {
    const auto& schema = schemas_.at(i);

    if (schema->IsKey()) {
      const auto& column = record.at(i);
      schema->EncodeKey(column, buf);
    }
  }

  EncodeCodecVersion(buf);

  buf.GetString(output);
  return output.size();
}

int RecordEncoderV2::EncodeValue(const std::vector<std::any>& record,
                                 std::string& output) {
  Buf buf(kBufInitCapacity, this->le_);

  // get total value size.
  int col_cnt = 0;
  for (const auto& schema : schemas_) {
    col_cnt += (schema->IsKey() ? 0 : 1);
  }

  EncodeSchemaVersion(buf);

  int cnt_not_null_col = 0;
  int cnt_null_col = 0;

  int cnt_not_null_col_pos = 4;
  int cnt_null_col_pos = cnt_not_null_col_pos + 2;
  int ids_pos = cnt_null_col_pos + 2;
  int offset_pos = ids_pos + col_cnt * 2;
  int data_pos = offset_pos + col_cnt * 4;

  buf.ReSize(data_pos);

  // append data.
  for (const auto& schema : schemas_) {
    if (!schema->IsKey()) {
      int index = schema->GetIndex();
      const auto& column = record.at(index);
      if (schema->isNull(column)) {
        cnt_null_col++;

        // write id
        buf.WriteShort(ids_pos, index);
        ids_pos += 2;

        // write offset
        buf.WriteInt(offset_pos, -1);
        offset_pos += 4;
      } else {
        cnt_not_null_col++;

        // write id
        buf.WriteShort(ids_pos, index);
        ids_pos += 2;

        // write offset
        buf.WriteInt(offset_pos, data_pos);
        offset_pos += 4;

        // write data.
        data_pos += schema->EncodeValue(column, buf);
      }
    }
  }

  buf.WriteShort(cnt_not_null_col_pos, cnt_not_null_col);
  buf.WriteShort(cnt_null_col_pos, cnt_null_col);

  buf.GetString(output);
  return output.size();
}

int RecordEncoderV2::EncodeMaxKeyPrefix(char prefix,
                                        std::string& output) const {
  if (common_id_ == INT64_MAX) {
    return -1;
  }

  Buf buf(kBufInitCapacity, this->le_);
  buf.Write(prefix);
  buf.WriteLong(common_id_ + 1);

  buf.GetString(output);
  return output.size();
}

int RecordEncoderV2::EncodeMinKeyPrefix(char prefix,
                                        std::string& output) const {
  Buf buf(kBufInitCapacity, this->le_);

  buf.Write(prefix);
  buf.WriteLong(common_id_);

  buf.GetString(output);
  return output.size();
}

}  // namespace V2
}  // namespace dingodb
