#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Run this script at NEMO_HOME as `bin/install_nemo.sh`

if [[ "$OSTYPE" == "linux-gnu"* ]]; then # Linux
  # Install prerequisites (maven, java8, protobuf)
  sudo add-apt-repository ppa:snuspl/protobuf-250
  sudo apt update
  sudo apt install maven openjdk-8-jdk protobuf-compiler=2.5.0-9xenial1

elif [[ "$OSTYPE" == "darwin"* ]]; then # Mac OSX
  # Install or update brew
  which -s brew
  if [[ $? != 0 ]] ; then
      # Install Homebrew
      ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
  else
      brew update
  fi

  # Install prerequisites (maven, java8, protobuf)
  brew install maven
  brew install --cask adoptopenjdk/openjdk/adoptopenjdk8
  wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.bz2
  tar xvf protobuf-2.5.0.tar.bz2
  pushd protobuf-2.5.0
  ./configure CC=clang CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g' LDFLAGS='-stdlib=libc++' LIBS="-lc++ -lc++abi"
  make -j 4
  sudo make install
  popd
fi

mvn clean install -T1C
