# Committing to Nemo

## Becoming a Committer

To get started contributing to Nemo, refer to the [contributor's guide](CONTRIBUTING.md).

Based on the contributions of the contributors for Nemo, PMC regularly adds new committers from the active contributors.
The qualifications for new committers include

1. Sustained contributions to Nemo
2. Quality of contributions
3. Community involvement

The type and level of contributions considered may vary by project area - code itself, documentations, platform support for specific OSes, storage systems, etc.

## How to Merge a Pull Request

Changes pushed to the master branch on Apache cannot be removed; that is, we can’t force-push to it. So please don’t add any test commits or anything like that, only real patches.

The easiest way to merge Pull Requests on Nemo is to use the GitHub Pull Requests interface.
The Pull Request interface looks something like below.



Upon merging, GitHub checks if the CI tests show successful runs, and blocks the merge if the tests fail.
With adequate approvals from the existing committers, you will be able to see the squash and merge button as below.
If you see something else, please use the 'Squash and merge' option, by clicking on the arrow next to the button.



After clicking on the button, you should see the options to fill out the commit title and the contents.



Please copy & paste the title of the PR, that has been shown at the top of the PR interface, and the contents, after clicking on the 'Edit' button, which you can see by clicking on the ... button.



After then, you can perform a successful merge! Congrats! :tada:
