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

![image](https://user-images.githubusercontent.com/6691311/51306580-05af7900-1a81-11e9-99b0-9a3c50d79cca.png)

Upon merging, GitHub checks if the CI tests show successful runs, and blocks the merge if the tests fail.
It also checks if it has an approving review, as well as if the branch is up-to-date with the master branch.
If the merge can be handled without any conflicts, GitHub provides the 'Update branch' button, which automatically merges the master branch straight from the GitHub UI.

![image](https://user-images.githubusercontent.com/6691311/51365119-224fbd80-1b22-11e9-8acf-fa5f310b45a6.png)

With adequate approvals from the existing committers, as well as other status checks, you will be able to see the squash and merge button as below.
If you see something else, please use the 'Squash and merge' option, by clicking on the green arrow button next to the green button describing the action.

![image](https://user-images.githubusercontent.com/6691311/51306208-1d3a3200-1a80-11e9-9b4f-1bfb3b234681.png)

After clicking on the button, you should see the options to fill out the commit title and the contents.

![image](https://user-images.githubusercontent.com/6691311/51306704-5fb03e80-1a81-11e9-965a-314079c8713c.png)

Please copy & paste the title of the PR, that has been shown at the top of the PR interface, and the PR descriptions.
You can get the PR description in full text by clicking on the ... button of the PR description and clicking on the 'Edit' button.

![image](https://user-images.githubusercontent.com/6691311/51306884-b6b61380-1a81-11e9-8694-94d340f3f2fc.png)

After then, by hitting 'Confirm squash and merge', you can perform a successful merge! Congrats! :tada:
