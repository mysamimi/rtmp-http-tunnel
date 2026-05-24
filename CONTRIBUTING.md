# Contributing to rtmp-http-tunnel

First off, thank you for considering contributing to `rtmp-http-tunnel`! It's people like you who make the open-source community such an amazing place to learn, inspire, and create.

## Code of Conduct
By participating in this project, you agree to maintain a respectful, welcoming, and collaborative environment.

## How Can I Contribute?

### Reporting Bugs
If you find a bug, please open an issue on GitHub. Include:
- A clear and descriptive title.
- Steps to reproduce the issue.
- Your configuration file (excluding sensitive tokens/keys).
- Error logs from the client/server or FFmpeg outputs.
- Your environment details (OS, Go version, FFmpeg version).

### Suggesting Enhancements
We welcome feature requests! Open an issue detailing:
- The behavior you would like to see.
- Why this enhancement would be useful for the project.

### Pull Requests
Ready to submit code? Please follow these steps:
1. **Fork** the repository and create your branch from `main`.
2. Ensure your code follows standard Go formatting by running:
   ```bash
   go fmt ./...
   ```
3. Run tests to ensure no regressions:
   ```bash
   go test ./...
   ```
4. Verify your changes build cleanly:
   ```bash
   go build
   ```
5. Commit your changes with clear, descriptive commit messages.
6. Push to your fork and submit a pull request (PR) to the `main` branch.

## Development Environment Setup

To work on this project locally, you will need:
- **Go** (1.20+)
- **FFmpeg** (installed and in your path)

### Running tests
```bash
go test -v ./...
```
