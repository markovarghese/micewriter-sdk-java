# AI Agent Instructions

You are working on the `micewriter-sdk-java` repository. 

## Git Commit Rules
**CRITICAL**: You MUST format all of your git commit messages according to the **Conventional Commits** specification.
A GitHub Action (`commitlint`) will reject your pull requests if you do not follow this format!

Format: `<type>(<optional scope>): <description>`

Allowed types:
- `feat`: A new feature (correlates with MINOR version bump)
- `fix`: A bug fix (correlates with PATCH version bump)
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, etc)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `build`: Changes that affect the build system or external dependencies
- `ci`: Changes to CI configuration files and scripts
- `chore`: Other changes that don't modify src or test files

Example:
`feat: extract IcebergEntity annotation to api module`
`fix: resolve dependency cycle in bom module`

If you introduce a breaking API change, append `!` to the type:
`feat!: drop java 11 support`
