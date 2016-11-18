LibLogCabin

Raft Consensus in C++, based on LogCabin's implementation

== Build pre-requisites ==
- scons
- g++
- protobuf
- cryptopp
- folly

On Ubuntu packages can be installed with:

% sudo apt-get install scons build-essential protobuf-compiler libprotobuf-dev autoconf

cryptopp can be installed from source:
% git clone http://github.com/tnachen/cryptocpp; cd cryptocpp; git checkout 5_6_1_fixes; make
% sudo make install

folly also can be installed from source (follow folly's README for pre-reqs):
% git clone http://github.com/facebook/folly; cd folly/folly; git checkout v0.57.0;
% autoreconf -ivf && ./configure && make
% sudo make install
% sudo ldconfig

== Build instructions ==
Go into liblogcabin parent folder and run:
% scons

== Tests ==
After building, run the test program
% ./build/test/test

Or running particular test(s):
% ./build/test/test --gtest_filter="*RaftIntegration*"