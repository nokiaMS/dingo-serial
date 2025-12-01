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

#include "decimal_schema.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "serial/utils/V2/compiler.h"
#include "serial/utils/serializer/decimal/MyDecimal.h"

namespace dingodb {
namespace serialV2 {

const int kGroupSize = 8;
const int kPadGroupSize = 9;
const uint8_t kMarker = 255;

int DingoSchema<DecimalString>::EncodeBytesNotComparable(const std::string& data,
                                                       Buf& buf) {
  buf.WriteInt(data.size());
  buf.WriteString(data);

  return data.size() + 4;
}

void DingoSchema<DecimalString>::DecodeBytesNotComparable(Buf& buf,
                                                        std::string& data) {
  int size = buf.ReadInt();
  data.resize(size);
  for (int i = 0; i < size; ++i) {
    data[i] = buf.Read();
  }
}

void DingoSchema<DecimalString>::DecodeBytesNotComparable(Buf& buf,
                                                        std::string& data,
                                                        int offset) {
  int size = buf.ReadInt(offset);
  data.resize(size);
  offset += 4;
  for (int i = 0; i < size; ++i) {
    data[i] = buf.Read(offset++);
  }
}

int DingoSchema<DecimalString>::GetLengthForKey() {
  return 100;  //A preferred length. The buffer should be enlarged if it is not enough.
}

int DingoSchema<DecimalString>::GetLengthForValue() {
  throw std::runtime_error("String unsupport length");
}

int DingoSchema<DecimalString>::SkipKey(Buf& buf) {
  if (AllowNull()) {
    if (buf.Read() == k_null) {
      return 1;
    }

    int size = buf.ReverseReadInt();  //get length.
    buf.Skip(size);

    return size + 1;  // with null flag.
  } else {
    int size = buf.ReverseReadInt();  //get length.
    buf.Skip(size);

    return size;
  }
}

int DingoSchema<DecimalString>::SkipValue(Buf& buf) {
  int size = buf.ReadInt();
  buf.Skip(size);

  return size + 4;
}

int DingoSchema<DecimalString>::EncodeBytesComparable(const std::string& data,
                                                    Buf& buf) {
  for (uint32_t i = 0; i < data.size(); ++i) {
    buf.Write(data.at(i));
    if ((i + 1) % kGroupSize == 0) {
      buf.Write(kMarker);
    }
  }

  int group_num = data.size() / kGroupSize + 1;
  int pad_count = group_num * kGroupSize - data.size();
  for (int i = 0; i < pad_count; ++i) {
    buf.Write(0);
  }
  buf.Write(kMarker - pad_count);

  int len = data.size();
  int encodedLen = group_num * 9;

  return group_num * 9;
}

int DingoSchema<DecimalString>::DecodeBytesComparable(Buf& buf,
                                                    std::string& data) {
  int size = 0;
  for (;;) {
    if (buf.RestReadableSize() < kPadGroupSize) {
      return -1;
    }

    uint8_t marker = buf.Read(buf.ReadOffset() + kGroupSize);

    int pad_count = kMarker - marker;
    for (int i = 0; i < kGroupSize - pad_count; ++i) {
      data.push_back(buf.Read());
    }

    size += kPadGroupSize;
    if (pad_count != 0) {
      for (int i = 0; i < pad_count; ++i) {
        if (buf.Read() != 0) {
          return -1;
        }
      }
      buf.Skip(1);  // skip marker

      break;
    }

    buf.Skip(1);  // skip marker
  }

  return size;
}

int DingoSchema<DecimalString>::internalEncodeKey(std::string& data, Buf& buf) {
  MyDecimal myDecimal = MyDecimal(data, (int)precision_, (int)scale_);
  std::string toBinValue = myDecimal.toBin();

  int len = EncodeBytesComparable(toBinValue, buf);
  return len;
}

int DingoSchema<DecimalString>::EncodeKey(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("data not has value.");
  }
  if (AllowNull()) {
    if (data.has_value()) {
      buf.Write(k_not_null);
      auto ref_data = std::any_cast<const std::string&>(data);
      int length = internalEncodeKey(ref_data, buf);
      buf.ReverseWriteInt(length);
      return length;
    } else {
      buf.Write(k_null);
      return 1;
    }
  } else {
    if (data.has_value()) {
      auto ref_data = std::any_cast<const std::string&>(data);
      int length = internalEncodeKey(ref_data, buf);
      buf.ReverseWriteInt(length);
      return length;
    } else {
      return 0;
    }
  }
}

void DingoSchema<DecimalString>::EncodeKeyPrefix(const std::any& data, Buf& buf) {
  EncodeKey(data, buf);
}

int DingoSchema<DecimalString>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("data not has value.");
  }

  if (data.has_value()) {
    const auto& ref_data = std::any_cast<const std::string&>(data);
    return EncodeBytesNotComparable(ref_data, buf);
  }

  return 0;
}

std::string DingoSchema<DecimalString>::internalReadDecimal(Buf& buf) {
  std::string data;
  DecodeBytesComparable(buf, data);

  return MyDecimal(data, (int)this->precision_, (int)this->scale_, true).decimalToString();
}

std::any DingoSchema<DecimalString>::DecodeKey(Buf& buf) {
  if (AllowNull()) {
    if (buf.Read() == k_null) {
      return std::any();
    }
  }

  std::string data = internalReadDecimal(buf);
  return std::move(std::any(std::move(data)));
}

std::any DingoSchema<DecimalString>::DecodeValue(Buf& buf) {
  std::string data;
  DecodeBytesNotComparable(buf, data);

  return std::move(std::any(std::move(data)));
}

std::any DingoSchema<DecimalString>::DecodeValue(Buf& buf, int offset) {
  std::string data;
  DecodeBytesNotComparable(buf, data, offset);

  return std::move(std::any(std::move(data)));
}

}  // namespace serialV2
}  // namespace dingodb
