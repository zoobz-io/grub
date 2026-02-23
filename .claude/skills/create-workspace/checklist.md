# Workspace Create Checklist

## Phase 1: Discovery

### Package Understanding
- [ ] What is the core abstraction? (interface that providers implement)
- [ ] What providers are needed?
- [ ] What are the heavy dependencies for each provider?

### Provider Inventory
For each provider, identify:
- [ ] Provider name (e.g., redis, postgres, kafka)
- [ ] Primary SDK dependency
- [ ] Testing approach (testcontainers, embedded, mock)

## Phase 2: Root Module

### go.mod (Root)

Keep minimal — only core dependencies:

```go
module github.com/zoobzio/[package]

go 1.24

toolchain go1.25.x

require (
    // Only essential dependencies for core interfaces
    // NO provider-specific SDKs here
)
```

### Core Files
- [ ] `api.go` — Core interfaces that providers implement
- [ ] `[core].go` — Core implementation/utilities
- [ ] `[core]_test.go` — Core tests

## Phase 3: Provider Modules

For each provider, create a separate module:

### Directory Structure

```
[provider]/
├── go.mod
├── go.sum
├── [provider].go      # Implementation
└── [provider]_test.go # Tests (may use testcontainers)
```

### Provider go.mod

```go
module github.com/zoobzio/[package]/[provider]

go 1.24

toolchain go1.25.x

require (
    // Provider-specific SDK
    github.com/[org]/[sdk] vX.Y.Z

    // Testing dependencies (testcontainers if needed)
    github.com/testcontainers/testcontainers-go vX.Y.Z
)

// Local development reference
replace github.com/zoobzio/[package] => ../
```

### Provider Checklist
For each provider:
- [ ] Create `[provider]/` directory
- [ ] Create `[provider]/go.mod` with replace directive
- [ ] Create `[provider]/[provider].go` implementing core interface
- [ ] Create `[provider]/[provider]_test.go`
- [ ] Run `go mod tidy` in provider directory

## Phase 4: Go Workspace

### go.work

```go
go 1.24

toolchain go1.25.x

use (
    .
    ./[provider-a]
    ./[provider-b]
    ./[provider-c]
)
```

### Workspace Rules
- [ ] Root module included (`.`)
- [ ] All provider modules included
- [ ] `go.work.sum` generated (gitignore this)
- [ ] Verify `go work sync` succeeds

### .gitignore Addition

```gitignore
# Workspace
go.work.sum
```

## Phase 5: Testing Package

Testing utilities live in root module, NOT as a separate module:

### Structure

```
testing/
├── helpers.go        # Shared test utilities
├── helpers_test.go   # Helper tests
├── integration/
│   └── README.md
└── benchmarks/
    └── README.md
```

### Why Not a Separate Module?

- Avoids circular dependency issues
- Consumers import `github.com/zoobzio/[package]/testing` naturally
- Test helpers don't bring heavy deps (those stay in providers)

### helpers.go

```go
//go:build testing

package testing

// Provide mocks, fixtures, assertions for the core interfaces
// Do NOT import provider-specific dependencies here
```

## Phase 6: Makefile Updates

Update Makefile to work with workspace:

```makefile
test: ## Run all tests (workspace-aware)
	@go test -v -race ./...

test-provider: ## Run tests for a specific provider
	@go test -v -race ./$(PROVIDER)/...

tidy: ## Tidy all modules
	@go mod tidy
	@for dir in */go.mod; do (cd $$(dirname $$dir) && go mod tidy); done

sync: ## Sync workspace
	@go work sync
```

## Phase 7: CI Configuration

### Workspace Transparency

Go workspaces are transparent to CI — `go test ./...` works across all modules automatically. No special initialization needed.

### ci.yml

```yaml
name: CI

on:
  push:
    branches: [ main, master ]
  pull_request:
    branches: [ main, master ]

jobs:
  test:
    name: Test
    runs-on: ubuntu-latest
    strategy:
      matrix:
        go-version: ['1.24', '1.25']
    steps:
    - uses: actions/checkout@v4
    - name: Set up Go
      uses: actions/setup-go@v5
      with:
        go-version: ${{ matrix.go-version }}
    # Workspace makes this work across all modules
    - name: Test all modules
      run: go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

  lint:
    name: Lint
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - name: Set up Go
      uses: actions/setup-go@v5
      with:
        go-version: '1.25'
    - name: golangci-lint
      uses: golangci/golangci-lint-action@v7
      with:
        version: v2.7.2
        args: --config=.golangci.yml --timeout=5m

  # ... security, benchmark jobs same as bedrock

  ci-complete:
    name: CI Complete
    needs: [test, lint, security, benchmark]
    runs-on: ubuntu-latest
    steps:
    - run: echo "CI complete"
```

### Module Validation (in release workflow)

```yaml
- name: Validate all go.mod files
  run: |
    go mod tidy
    git diff --exit-code go.mod go.sum || (echo "Root go.mod needs updating" && exit 1)
    for dir in */; do
      if [ -f "$dir/go.mod" ]; then
        (cd "$dir" && go mod tidy)
        git diff --exit-code "$dir/go.mod" "$dir/go.sum" || (echo "$dir/go.mod needs updating" && exit 1)
      fi
    done
```

## Phase 8: Release Configuration

### release.yml (Full Workspace Release)

```yaml
name: Release

on:
  push:
    tags:
      - 'v*.*.*'

concurrency:
  group: release-${{ github.ref }}
  cancel-in-progress: false

permissions:
  contents: write
  packages: write
  id-token: write

jobs:
  validate:
    name: Validate Module
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.25'

      - name: Validate go.mod
        run: |
          go mod tidy
          git diff --exit-code go.mod go.sum || (echo "go.mod/go.sum need updating" && exit 1)

      - name: Run tests
        run: go test -v -race ./...

      - name: Run linting
        uses: golangci/golangci-lint-action@v7
        with:
          version: v2.7.2
          args: --config=.golangci.yml --timeout=5m

  tag-submodules:
    name: Tag Submodules
    needs: validate
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.25'

      - name: Update and tag submodules
        run: |
          VERSION=${GITHUB_REF#refs/tags/}

          # Configure git for commits
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          # List your providers explicitly
          for mod in [provider-a] [provider-b] [provider-c]; do
            # Skip if tag already exists
            if git ls-remote --exit-code --tags origin "refs/tags/${mod}/${VERSION}" >/dev/null 2>&1; then
              echo "Tag ${mod}/${VERSION} already exists, skipping"
              continue
            fi

            # Update submodule go.mod to reference released root version
            cd "$mod"
            go mod edit -require="github.com/zoobzio/[package]@${VERSION}"
            go mod tidy
            cd ..

            # Commit the go.mod update
            git add "${mod}/go.mod" "${mod}/go.sum"
            git commit -m "chore(${mod}): update root dependency to ${VERSION}"

            # Tag at the new commit
            git tag "${mod}/${VERSION}"
          done

          # Push commits and tags
          git push origin HEAD:main
          git push origin --tags

  release:
    name: Create Release
    needs: tag-submodules
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.25'

      - name: Run GoReleaser
        uses: goreleaser/goreleaser-action@v6
        with:
          version: latest
          args: release --clean
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          # Critical: tells GoReleaser which tag to use
          GORELEASER_CURRENT_TAG: ${{ github.ref_name }}

  verify:
    name: Verify Release
    needs: release
    runs-on: ubuntu-latest
    steps:
      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.25'

      - name: Get release tag
        id: get_tag
        run: echo "TAG=${GITHUB_REF#refs/tags/}" >> $GITHUB_OUTPUT

      - name: Verify module can be fetched
        run: |
          sleep 30  # Wait for module proxy to update
          cd $(mktemp -d)
          go mod init test
          go get github.com/zoobzio/[package]@${{ steps.get_tag.outputs.TAG }}

      - name: Release summary
        run: |
          echo "### Release ${{ steps.get_tag.outputs.TAG }} Complete!" >> $GITHUB_STEP_SUMMARY
          echo '```bash' >> $GITHUB_STEP_SUMMARY
          echo "go get github.com/zoobzio/[package]@${{ steps.get_tag.outputs.TAG }}" >> $GITHUB_STEP_SUMMARY
          echo '```' >> $GITHUB_STEP_SUMMARY
```

### Submodule Tag Format

When you tag `v1.2.3`, the workflow creates:
- `v1.2.3` (root module)
- `redis/v1.2.3`
- `kafka/v1.2.3`
- `nats/v1.2.3`
- etc.

### Consumer Import Paths

After release, consumers import specific providers:
```go
import "github.com/zoobzio/[package]/redis"
```

With versioning:
```bash
go get github.com/zoobzio/[package]/redis@v1.2.3
```

The prefixed tag (`redis/v1.2.3`) allows Go modules to resolve the correct version for the submodule.

## Phase 9: Verification

- [ ] `go work sync` succeeds
- [ ] `go build ./...` succeeds from root
- [ ] `go test ./...` runs all tests
- [ ] Each provider can be imported independently
- [ ] Provider imports don't bring other providers' deps
- [ ] Replace directives work for local development
- [ ] CI handles multi-module structure
