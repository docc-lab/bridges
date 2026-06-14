#!/bin/bash
ORT="/users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/"
g++ -std=c++17 -O2 -I"$ORT/include" validate.cc -L"$ORT/lib" -lortools -Wl,-rpath,"$ORT/lib" -o validate
