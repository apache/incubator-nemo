#!/bin/bash
#
# Copyright (C) 2018 Seoul National University
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
echo "./set_hostname.sh remote_hosts_file"
cat $1 | xargs -n 1 -I '{}' ssh '{}' "sudo bash -c 'echo '{}' > /etc/hostname'"
echo "Done! Please check /etc/hostname"
