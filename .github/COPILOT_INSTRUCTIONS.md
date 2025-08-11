# GitHub Copilot Instructions for SproutDB Project

## Project Overview

SproutDB is a revolutionary database system featuring advanced versioning capabilities, intuitive query language, and real-time updates.

## Naming Conventions

### Terminology Guidelines

1. **No "Git" References**:
   - Do not use the term "Git" in any generated code, comments, or documentation
   - Instead, use generic terms like "version control," "versioning," "branches," or "commits"
   - This helps maintain the unique identity of SproutDB and avoids confusion with Git

2. **Preferred Terminology**:
   - ✅ "Advanced Versioning" (not "Git-style versioning")
   - ✅ "Version History" (not "Git history")
   - ✅ "Branch-based development" (not "Git-like branches")
   - ✅ "Commit history" (not "Git commits")

3. **Acceptable Version Control Terms**:
   The following terms can be used as they are common in version control systems in general:
   - "Branches"
   - "Commits"
   - "Merge"
   - "History"
   - "Time Travel"
   - "Version"

### Schema Design

- SproutDB requires proper schema design
- Don't refer to SproutDB as "schema-less" or "zero-config schema"
- Use "flexible schema design" when referring to its schema capabilities

### Notifications

- Don't refer to "Branch-Specific Notifications"
- Use "Filtered Notifications" instead, which can filter by tables or operations

## Code Style Preferences

- Use camelCase for variable names
- Use PascalCase for class names
- Use meaningful, descriptive names for all identifiers
- Include comments for complex logic
- Follow the SOLID principles for object-oriented design

## Project Structure Expectations

- Separate concerns appropriately
- Keep related files together
- Follow modular design principles
- Use appropriate design patterns

## Specific Feature Implementations

- For database queries, follow the syntax documented in SproutDB's documentation
- For real-time updates, use the SignalR connection and subscription model

## Testing Expectations

- Write tests for all new functionality
- Test edge cases and error scenarios
- Use appropriate mocking for external dependencies

## Documentation Expectations

- Document all public APIs
- Include examples for complex functionality
- Keep documentation up-to-date with code changes
- Follow the naming conventions outlined above
