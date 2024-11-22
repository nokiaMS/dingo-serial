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

#include "boolean_schema.h"

#include <any>
#include <stdexcept>

#include "serial/utils/V2/compiler.h"

namespace dingodb {
namespace serialV2 {

/*
 * bool schema in key:
 *    null flag(1 byte) | bool value(1 byte)
 *    if null:
 *      0x00 | 0x00
 *    if not null:
 *      0x01 | true/false (1 byte)
 * bool schema in value:
 *    bool value(1 byte)
 *    if null:
 *      nothing encoded.
 *    if not null:
 *      true/false (1 byte)
 *
 */

// bool type length.
constexpr int kDataLength = 1;

// bool type length + null flag(1 byte)
constexpr int kDataLengthWithNull = kDataLength + 1;

inline int DingoSchema<bool>::GetLengthForKey() {
  if (AllowNull()) {
    return kDataLengthWithNull;
  } else {
    return kDataLength;
  }
}

inline int DingoSchema<bool>::GetLengthForValue() { return kDataLength; }

int DingoSchema<bool>::SkipKey(Buf& buf) {
  int len = GetLengthForKey();
  buf.Skip(len);
  return len;
}

int DingoSchema<bool>::SkipValue(Buf& buf) {
  buf.Skip(kDataLength);
  return kDataLength;
}

inline int DingoSchema<bool>::Encode(const std::any& data, Buf& buf,
                                     bool forKey) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    const auto& ref_data = std::any_cast<const bool&>(data);
    if (forKey) {
      buf.Write(k_not_null);
    }
    buf.Write(ref_data ? 0x1 : 0x0);
  } else {
    if (forKey) {
      buf.Write(k_null);
      buf.Write(0x0);
    } else {
      return 0;  // null will not be encoded in value.
    }
  }

  return forKey ? GetLengthForKey() : kDataLength;
}

int DingoSchema<bool>::EncodeKey(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (AllowNull()) {
    if (data.has_value()) {
      buf.Write(k_not_null);
      const auto& ref_data = std::any_cast<const bool&>(data);
      buf.Write(ref_data ? 0x1 : 0x0);
    } else {
      buf.Write(k_null);
      buf.Write(0x0);
    }

    return kDataLengthWithNull;
  } else {
    const auto& ref_data = std::any_cast<const bool&>(data);
    buf.Write(ref_data ? 0x1 : 0x0);

    return kDataLength;
  }
}

int DingoSchema<bool>::EncodeValue(const std::any& data, Buf& buf) {
  return Encode(data, buf, false);
}

std::any DingoSchema<bool>::DecodeKey(Buf& buf) {
  if (AllowNull()) {
    if (buf.Read() == k_null) {
      buf.Skip(kDataLength);  // The null flag has already been read.
      return std::any();
    }
  }

  return std::any(static_cast<bool>(buf.Read()));
}

std::any DingoSchema<bool>::DecodeValue(Buf& buf) {
  return std::any(static_cast<bool>(buf.Read()));
}

}  // namespace serialV2
}  // namespace dingodb