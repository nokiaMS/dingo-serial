//
// Created by guoxu on 2025/10/28.
//

#ifndef DINGO_STORE_COMMON_H
#define DINGO_STORE_COMMON_H

class DecimalString {
public:
  DecimalString() = default;

  DecimalString(int len, char v) : str(len, v) {
  }

  std::string str;
};

#endif  // DINGO_STORE_COMMON_H
