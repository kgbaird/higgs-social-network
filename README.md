## higgs-social-network

Explores relationships in Twitter data during experiments at Large Hadron collider

### Background

Data was collected by monitoring Twitter activity following announcements describing experiments at CERN's Large Hadron collider. Futher details of the experiments can be found on the [CERN website](https://home.cern/topics/higgs-boson)

### Source data

Data was pulled from the [Stanford's SNAP](https://snap.stanford.edu/data/higgs-twitter.html)

### Analysis tools

Analysis was performed using [Apache Spark](https://spark.apache.org/). [Python](https://www.python.org/) was used for the orchestration

### Development guidelines

  - We intend to utilize [GitHub flow](https://guides.github.com/introduction/flow/) as our branching strategy. That means developers should not make direct commits to the `master` branch. Instead, work should be completed in branches off of the `master` branch and submitted via pull requests to encourage collaboration
  - Wikis have been disabled for this repository to encourage developers to document within code
  - Currently, tests are not required and CI has not been configured because the code in this repository is not anticipated to be library code. It is intended to be more ad hoc. However, should developers wish to submit tests with their work, we encourage the use of the [pytest](https://docs.pytest.org/en/latest/) framework
  - Developers are encouraged to utilize [issues](https://github.com/kgbaird/higgs-social-network/issues) to track tasks and the [project board](https://github.com/kgbaird/higgs-social-network/projects/1) to visualize their workflow
