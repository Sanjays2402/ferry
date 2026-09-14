name: Bug report
description: Something isn't working as documented
title: "[bug] "
labels: [bug]
body:
  - type: markdown
    attributes:
      value: |
        Thanks for reporting! A minimal repro gets your issue fixed fastest.
  - type: input
    id: version
    attributes:
      label: Ferry version
      placeholder: "0.1.0"
    validations:
      required: true
  - type: input
    id: broker
    attributes:
      label: Broker
      description: "sqlite:// or redis:// (and version, if Redis)"
      placeholder: "sqlite:///ferry.db"
    validations:
      required: true
  - type: input
    id: python
    attributes:
      label: Python version
      placeholder: "3.12"
    validations:
      required: true
  - type: textarea
    id: repro
    attributes:
      label: Minimal reproduction
      description: The smallest script that shows the bug
      render: python
    validations:
      required: true
  - type: textarea
    id: expected
    attributes:
      label: Expected vs actual behavior
    validations:
      required: true
  - type: textarea
    id: traceback
    attributes:
      label: Traceback / logs
      render: text
