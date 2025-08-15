# Claude Development Guidelines

## Compilation and Testing Requirements

**IMPORTANT**: Before marking any development task as complete, you MUST:

1. **Verify compilation** by running:

   ```bash
   cargo check
   ```

2. **Run all tests** and ensure they pass:

   ```bash
   cargo test --release
   ```

3. If you're working on multiple todos, verify compilation and tests after each significant change or todo completion.

4. If compilation fails or tests break, fix the issues before proceeding to the next task.

5. Never mark a todo as "completed" without verifying that the code compiles and all tests pass.

## Development Best Practices

- Always verify that your changes don't break existing functionality
- Run tests incrementally when making multiple changes
- Use `cargo check` for quick compilation verification during development
- Use `cargo test --release` for full test suite verification before completing tasks

## Code Quality Standards

**NO SHORTCUTS**: All code must be production-ready and properly implemented.

1. **No Fallback to Simpler Solutions**: When facing implementation challenges, solve them properly rather than reverting to simpler, less optimal approaches.

2. **Maximum Performance**: Write code for optimal performance. This includes:

   - Using efficient algorithms and data structures
   - Minimizing allocations and copies
   - Leveraging concurrency where appropriate
   - Avoiding unnecessary iterations

3. **Security First**: All code must be written with security in mind:

   - Proper input validation
   - Protection against timing attacks
   - Secure cryptographic practices

4. **No Placeholders**: Never use placeholder implementations or comments like:

   - "In a production system..."
   - "TODO: implement properly"
   - "This is a simplified version..."
   - Mock implementations that don't actually work

5. **Complete Solutions**: Every implementation must be:
   - Fully functional
   - Properly tested
   - Ready for production use
   - Maintainable and well-structured

## Code Addition Guidelines

**CRITICAL**: Never add unused code to the codebase. Follow these strict guidelines:

### **Prohibited Practices:**

1. **No Unused Functions**: Never create functions, methods, or constants that are not actively used in the codebase
2. **No Dead Code**: Do not leave commented-out code, unused imports, or unreachable code paths
3. **No "Future" Features**: Do not add functionality "for future use" unless it's immediately integrated and tested
4. **No Orphaned Utilities**: Every utility function must have at least one active caller in the current codebase

### **Required Practices:**

1. **Integration Mandatory**: When adding new functionality:

   - **MUST** be properly integrated into existing code flows
   - **MUST** have active callers or be part of public APIs that are used
   - **MUST** be covered by tests that demonstrate actual usage

2. **Verification Steps**: Before adding any new code:

   - Identify exactly where and how it will be used
   - Implement the caller/integration point first
   - Verify the new code is actually exercised by existing functionality
   - Run `cargo check --tests` to ensure no "unused" warnings for your additions

3. **Maintenance Integration**: New maintenance or cleanup functions:
   - **MUST** be called from appropriate places (startup, periodic tasks, shutdown, etc.)
   - **MUST** be integrated into existing maintenance loops where applicable
   - **MUST** be covered by integration tests or have clear usage patterns

### **Examples of Proper Integration:**

✅ **GOOD**: Adding `perform_maintenance()` method AND integrating it into `NetworkManager::maintain_peers()`
✅ **GOOD**: Creating helper functions that are immediately used by existing public methods  
✅ **GOOD**: Adding constants that are actively referenced by current algorithms

❌ **BAD**: Creating utility functions "that might be useful later"
❌ **BAD**: Adding constants that are not referenced anywhere
❌ **BAD**: Implementing methods that have no current callers
❌ **BAD**: Creating "framework" code without immediate concrete usage

### **Code Review Checklist:**

Before completing any development task, verify:

- [ ] No compiler warnings about unused code
- [ ] All new functions/methods have active callers
- [ ] All new constants are actually used
- [ ] Integration points are properly tested
- [ ] Documentation reflects actual usage, not theoretical usage
