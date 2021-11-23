
# Py Load Limiter

A highly configurable, feature-packed, variable-request-sized load limiter python module.

## Features

- Sliding window/buckets algorithm
- Limit amount of 'load' instead of simply limiting the number of requests by allowing load-aware requests
- Use with direct apis, as context manager or as decorator
- Support for automatic retry, delay or timeout on load requests
- Automatically compute TTA (time-to-availability) to easily give clients an amount of time to wait before resubmissions
- Configurable penalties for over-max-load requests and for uncompliant clients who do not respect the required delays
- Composite load limiter to allow for complex load limiting with a single instance (eg. long-time rate limiting together with burst protection)
- Configurable window fragmentation for optimal smoothness vs performance tuning
- Pluggable and customizable storage adapters to allow for easy persistence