# Contributing to Nemo 

:tada: Thanks for taking the time to contribute! :tada:

This project and everyone participating in it is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

Before contributing to our project, keep in mind that we go through the following simple steps:

- Identify the change required for the project.
- Search and check for existing, related [JIRA tickets](https://issues.apache.org/jira/projects/NEMO/issues) and [pull requests](https://github.com/apache/incubator-nemo/pulls). Make a new JIRA ticket if the problem is not pointed out.
- Make sure that the change is important and ready enough for the community to spend time reviewing
- Open the pull request following the [PR template](ㄱpull_request_template.md), clearly explaining and motivating the change.

When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project's open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project's open source license and warrant that you have the legal authority to do so.

## Things to know before getting started

- [Our Docs](http://nemo.apache.org/docs/home/)
- [Apache Beam](https://beam.apache.org/)
- [Apache Spark](http://spark.apache.org/)
- [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
- [Apache REEF](http://reef.apache.org/)
- Big Data Workloads like [MR](https://en.wikipedia.org/wiki/MapReduce), [PageRank](https://en.wikipedia.org/wiki/PageRank), [Multinomial Logistic Regression](https://en.wikipedia.org/wiki/Multinomial_logistic_regression)..

## How to contribute

- Reporting Bugs
- Suggesting Enhancements
- Reviewing Pull Requests and Changes
- Documentation Changes (website documentations can be changed through the links on our [website](http://nemo.apache.org/))
- JIRA Maintenance
- Code Contribution

## Pull Request Process

1. [Fork](https://github.com/apache/incubator-nemo#fork-destination-box) the GitHub repository at `https://github.com/apache/incubator-nemo`.
2. Make sure the changes are required and discussed through [JIRA](https://issues.apache.org/jira/projects/NEMO/issues), our issue tracker.
3. Clone your fork, create a new branch like `ISSUE#-SHORT_TITLE` (e.g. `25-WebUI`), push commits to the branch.
4. Consider whether documentations or tests are needed as part of the change, and add them if needed.
5. Run `mvn clean install` to verify that the code runs and tests pass.
6. [Open a pull request](https://github.com/apache/incubator-nemo/pull/new/master) following the [PR template](pull_request_template.md).
  - the PR title should be of form `[NEMO-##] Title`, specifying the relevant JIRA ticket number, and a short description of the change.
  - if the PR is still a work in progress and is not ready to be merged, add `[WIP]` before the title.
  - Consider identifying the reviewer of the PR, with the suggestions provided by GitHub.
