//
// Created by guoxu on 2025/11/11.
//

#ifndef DINGO_SERIAL_GX_H
#define DINGO_SERIAL_GX_H

#include "serial/utils/V2/buf.h"

namespace dingodb {
namespace serialV2 {
class KeyBuf : public Buf {
private:
  int forward_pos_ = 0;

public:
  KeyBuf(int size, bool le);
  KeyBuf(const std::string& buf, bool le);
  void Init(int size);
  void Init(const std::string& buf);

  void Write(uint8_t data);
  void WriteShort(int16_t data);
  void WriteInt(int32_t data);
  void WriteLong(int64_t data);
  void WriteString(const std::string& data);

  void WriteWithNegation(uint8_t data);
  void WriteLongWithNegation(int64_t data);
  void WriteLongWithFirstBitNegation(int64_t data);

  void EnsureRemainder(int length);
};

}
}

#endif  // DINGO_SERIAL_GX_H
