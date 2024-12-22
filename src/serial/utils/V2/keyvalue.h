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

#ifndef DINGO_SERIAL_KEYVALUE_V2_H_
#define DINGO_SERIAL_KEYVALUE_V2_H_

#include <string>

namespace dingodb {
namespace serialV2 {

class KeyValue {
 public:
  KeyValue() = default;
  KeyValue(const std::string& key, const std::string& value);
  ~KeyValue() = default;

  void Set(const std::string& key, const std::string& value);
  void SetKey(const std::string& key);
  void SetValue(const std::string& value);

  int GetVersion() const { return this->key_.at(key_.size() - 1); }

  const std::string& GetKey() const;
  const std::string& GetValue() const;

 private:
  std::string key_;
  std::string value_;
};

}  // namespace serialV2
}  // namespace dingodb

#endif