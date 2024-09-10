cd flex
mkdir build
cd build && cmake .. -DBUILD_DOC=OFF -DCMAKE_EXPORT_COMPILE_COMMANDS=1 && make -j