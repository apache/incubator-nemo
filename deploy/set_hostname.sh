#!/bin/bash
echo "./set_hostname.sh remote_hosts_file"
cat $1 | xargs -n 1 -I '{}' ssh '{}' "sudo bash -c 'echo '{}' > /etc/hostname'"
echo "Done! Please check /etc/hostname"
