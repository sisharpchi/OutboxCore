# Contributing to OutboxCore

Thank you for your interest in contributing to OutboxCore! We welcome contributions of all kinds, including bug fixes, feature requests, documentation improvements, and feedback.

---

## Development Setup

To build and test the project locally, ensure you have the following prerequisites installed:
- [.NET 8.0 SDK / .NET 9.0 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop) (required to run integration tests)

### Step-by-Step Setup:
1. Fork the repository and clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/OutboxCore.git
   cd OutboxCore
   ```
2. Build the solution:
   ```bash
   dotnet build
   ```
3. Run the unit test suite:
   ```bash
   dotnet test tests/OutboxCore.Tests.Unit/OutboxCore.Tests.Unit.csproj
   ```
4. Run the integration test suite (requires Docker daemon running):
   ```bash
   dotnet test tests/OutboxCore.Tests.Integration/OutboxCore.Tests.Integration.csproj
   ```

---

## Submitting Pull Requests (PRs)

1. **Create a Branch**: Always create a new branch for your work (e.g. `feature/your-feature-name` or `bugfix/issue-id`).
2. **Write Tests**: Ensure any bug fixes or new features are accompanied by corresponding unit or integration tests.
3. **Verify Build**: Verify that all tests pass and the code compiles without warnings.
4. **Submit PR**: Open a Pull Request targeting the `main` branch. Provide a clear description of the problem solved or the feature added.

---

## Coding Standards

- Follow standard C# styling guidelines.
- Use meaningful variable, class, and method names.
- Document complex classes and public APIs using XML documentation comments.
