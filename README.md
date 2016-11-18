LibLogCabin

Raft Consensus in C++, based on LogCabin's implementation

== Build pre-requisites ==
- scons
- g++
- protobuf
- cryptopp

On Ubuntu packages can be installed with:

% sudo apt-get install scons build-essential protobuf-compiler libprotobuf-dev

cryptopp can be installed from source:
% git clone http://github.com/tnachen/cryptocpp; cd cryptocpp; git checkout 5_6_1_fixes; make
% sudo make install

== Build instructions ==
Go into liblogcabin parent folder and run:
% scons

== Tests ==
After building, run the test program
% ./build/test/test

Or running particular test(s):
% ./build/test/test --gtest_filter="*RaftIntegration*"