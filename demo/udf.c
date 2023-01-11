#include <stdlib.h>
#include <emscripten.h>

int add(int a0, int a1) {
    return a0 + a1;
}

void exec(int *input0, int *input1, int *output, int len) {
    for (int i = 0; i < len; i++) {
        output[i] = add(input0[i], input1[i]);
    }
}