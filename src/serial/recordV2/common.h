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

#ifndef DINGO_COMMON_H_
#define DINGO_COMMON_H_

#include <cstdint>

#include "serial/utilsV2/buf.h"

namespace dingodb {
namespace V2 {
enum idUnit { ID_1_BYTE = 0x01, ID_2_BYTE = 0x02 };

enum offsetUnitFlag { OFFSET_2_BYTE = 0x02, OFFSET_4_BYTE = 0x04 };

enum codecVersion { CODEC_VERSION_V1 = 0x01, CODEC_VERSION_V2 = 0x02 };

inline int CalcIdUnit(int not_null_id_cnt, int null_id_cnt) {
  return (not_null_id_cnt + null_id_cnt) < 255 ? ID_1_BYTE : ID_2_BYTE;
}

inline void WriteCountInfo(Buf& buf, int pos, int not_null_cnt, int null_cnt) {
  buf.WriteShort(pos, not_null_cnt);
  buf.WriteShort(pos + 2, null_cnt);
}

inline void GetCountInfo(Buf& buf, int& pos, int& not_null_cnt, int& null_cnt) {
  null_cnt = buf.ReadShort(pos - 2);
  not_null_cnt = buf.ReadShort(pos - 4);
  pos -= 4;
}

inline void WriteOffsetUnit(Buf& buf, const int pos,
                            const uint8_t offset_unit) {
  buf.WriteByte(pos, offset_unit);
}

inline void GetOffsetUnit(Buf& buf, int& pos, int& offset_unit) {
  offset_unit = buf.Read(pos - 1);
  pos -= 1;
}

}  // namespace V2
}  // namespace dingodb

#endif  // DINGO_COMMON_H_
