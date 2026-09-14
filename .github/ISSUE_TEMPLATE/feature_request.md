name: Feature request
description: Propose an idea for Ferry
title: "[feat] "
labels: [enhancement]
body:
  - type: textarea
    id: problem
    attributes:
      label: What problem does this solve?
      description: Describe the use case, not just the API you want
    validations:
      required: true
  - type: textarea
    id: proposal
    attributes:
      label: Proposed API
      description: Sketch the code you'd like to write
      render: python
  - type: checkboxes
    id: scope
    attributes:
      label: Scope check
      options:
        - label: "This works with both the SQLite and Redis brokers"
        - label: "This adds no required dependencies to the core install"
        - label: "I'm willing to submit a PR with tests"
