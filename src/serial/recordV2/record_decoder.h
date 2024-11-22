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

#ifndef DINGO_SERIAL_RECORD_DECODER_V2_H_
#define DINGO_SERIAL_RECORD_DECODER_V2_H_

#include <map>
#include <memory>

#include "any"
#include "common.h"
#include "serial/schemaV2/double_list_schema.h"
#include "serial/utilsV2/keyvalue.h"
#include "serial/utilsV2/utils.h"

namespace dingodb {
namespace V2 {

class RecordDecoderV2;
using RecordDecoderPtr = std::shared_ptr<RecordDecoderV2>;

class RecordDecoderV2 {
 public:
  RecordDecoderV2(int schema_version, const std::vector<BaseSchemaPtr>& schemas,
                  long common_id);
  RecordDecoderV2(int schema_version, const std::vector<BaseSchemaPtr>& schemas,
                  long common_id, bool le);

  static RecordDecoderPtr New(int schema_version,
                              const std::vector<BaseSchemaPtr>& schemas,
                              long common_id) {
    return std::make_shared<RecordDecoderV2>(schema_version, schemas,
                                             common_id);
  }

  bool dataIsNull(int id);
  int Decode(const KeyValue& key_value,
             std::vector<std::any>& record /*output*/);
  int Decode(const std::string& key, const std::string& value,
             std::vector<std::any>& record /*output*/);
  int Decode(std::string&& key, std::string&& value,
             std::vector<std::any>& record /*output*/);
  int DecodeKey(const std::string& key,
                std::vector<std::any>& record /*output*/);

  int Decode(const KeyValue& key_value, const std::vector<int>& column_indexes,
             std::vector<std::any>& record /*output*/);
  int Decode(const std::string& key, const std::string& value,
             const std::vector<int>& column_indexes,
             std::vector<std::any>& record /*output*/);
  int GetCodecVersion(Buf& buf) const;

 private:
  bool CheckPrefix(Buf& buf) const;
  bool CheckReverseTag(Buf& buf) const;
  bool CheckSchemaVersion(Buf& buf) const;

  bool le_;
  Buf key_buf_;
  Buf value_buf_;
  int codec_version_{CODEC_VERSION_V2};
  int schema_version_;
  long common_id_;

  std::vector<BaseSchemaPtr> schemas_;
};

}  // namespace V2
}  // namespace dingodb

#endif
