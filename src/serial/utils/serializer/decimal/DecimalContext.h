//
// Created by guoxu on 2025/11/10.
//

#ifndef DINGO_SERIAL_DECIMALCONTEXT_H
#define DINGO_SERIAL_DECIMALCONTEXT_H

/**
* max count of words in buf.
*/
static const int MaxWordBufLen = 9;

/**
 * max value of a word.
 */
static const int wordMax = 1000000000 - 1;

/**
 * dig mask.
 */
static const int digMask = 100000000;

/**
 * digits per word.
 */
static const int digitsPerWord = 9;

/**
 * word size.
 */
static const int wordSize = 4;

/**
 * word buffer length.
 */
static const int wordBufLen = 9;

/**
 * mysql max decimal scale.
 */
static const int MaxDecimalScale = 30;

/**
 * length of div9 array.
 */
static const int div9Len = 128;

/**
* div9 for digits to words.
*/
static int div9[128] = {
  0, 0, 0, 0, 0, 0, 0, 0, 0,
  1, 1, 1, 1, 1, 1, 1, 1, 1,
  2, 2, 2, 2, 2, 2, 2, 2, 2,
  3, 3, 3, 3, 3, 3, 3, 3, 3,
  4, 4, 4, 4, 4, 4, 4, 4, 4,
  5, 5, 5, 5, 5, 5, 5, 5, 5,
  6, 6, 6, 6, 6, 6, 6, 6, 6,
  7, 7, 7, 7, 7, 7, 7, 7, 7,
  8, 8, 8, 8, 8, 8, 8, 8, 8,
  9, 9, 9, 9, 9, 9, 9, 9, 9,
  10, 10, 10, 10, 10, 10, 10, 10, 10,
  11, 11, 11, 11, 11, 11, 11, 11, 11,
  12, 12, 12, 12, 12, 12, 12, 12, 12,
  13, 13, 13, 13, 13, 13, 13, 13, 13,
  14, 14
};

/**
* digits 2 bytes.
*/
static const int dig2bytesLength = 10;
static int dig2bytes[dig2bytesLength] = {
  0, 1, 1, 2, 2, 3, 3, 4, 4, 4,
};

/**
 * power10.
 */
static int powers10[10] = {
  1,
  10,
  100,
  1000,
  10000,
  100000,
  1000000,
  10000000,
  100000000,
  1000000000
};

enum DecimalError {
  None,
  ErrOverflow,
  ErrTruncate,
  ErrBadNumber
};

class DecimalContext {
public:
 int start;
 int end;
 int sum;
 int newCarry;
 int wordIdx;
 int digitsIntLocal;
 int newWordInt;
 int newWordFrac;
 int decimalBinSize;
 DecimalError err;

  DecimalContext() {
    start = 0;
    end = 0;
    sum = 0;
    newCarry = 0;
    wordIdx = 0;
    digitsIntLocal = 0;
    newWordInt = 0;
    newWordFrac = 0;
    decimalBinSize = 0;
    err = None;
  }
};

#endif  // DINGO_SERIAL_DECIMALCONTEXT_H
