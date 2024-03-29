<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## GitHub Workflows

This directory contains all Nemo CI checks. (Excerpt from the [Pulsar project](https://github.com/apache/pulsar/blob/master/.github/workflows/README.md))

### Required Workflows

This project uses the [.asf.yaml](../../.asf.yaml) to configure which workflows are required to pass before a PR can be merged.
When adding new CI workflows, please update the [.asf.yaml](../../.asf.yaml) if the workflow is required to pass before a PR can be merged.

You can view the currently required status checks by running the following command:

```shell
curl -s -H 'Accept: application/vnd.github.v3+json' https://api.github.com/repos/apache/incubator-nemo/branches/master | \
jq .protection
```

These contexts get their names in one of two ways depending on how the workflow file is written in this directory. The
following command will print out the names of each file and the associated with the check. If the `name` field is `null`,
the context will be named by the `id`.

```shell
for f in .github/workflows/*.yaml; \
do FILE=$f yq eval -o j '.jobs | to_entries | {"file": env(FILE),"id":.[].key, "name":.[].value.name}' $f; \
done
```

Duplicate names are allowed, and all checks with the same name will be treated the same (required or not required).

When working on workflow changes, one way to find out the names of the status checks is to retrieve the names
from the PR build run. The "check-runs" can be found by commit id. Here's an example:

```shell
curl -s "https://api.github.com/repos/apache/incubator-nemo/commits/$(git rev-parse HEAD)/check-runs" | \
  jq -r '.check_runs | .[] | .name' |sort
```
