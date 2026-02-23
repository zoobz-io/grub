# Recon

Establish ground truth about the environment and changes under review. This is reconnaissance, not review — understand what exists before evaluating it.

## Execution

1. Identify the current environment:
   - Run `git branch --show-current`
   - Run `git remote get-url origin`

2. Establish the diff against main:
   - Run `git log main..HEAD --oneline` for commit history on this branch
   - Run `git diff main...HEAD --stat` for what files changed and by how much
   - Run `git diff main...HEAD` for the actual changes

3. Understand the shape of the changes:
   - What packages are touched?
   - What is the apparent intent — new feature, refactor, bugfix, infrastructure?
   - What is the scope — narrow (one package) or broad (cross-cutting)?
   - Are there new files, deleted files, or only modifications?

4. Note anything surprising:
   - Files changed that seem unrelated to the apparent intent
   - Large diffs in unexpected places
   - Changes to configuration, dependencies, or build infrastructure

## What This Is

Ground truth. You are establishing facts about the environment so that when the review begins, you are not starting cold. You know the branch, the repo, the diff, and the shape of the changes.

## What This Is NOT

This is not a review. Do not evaluate code quality, test coverage, security posture, or architectural alignment during recon. Do not produce findings. Do not assign severity. You are gathering intelligence, not making judgments.

## Output

After recon, you should be able to state:
- What branch you are on and what repo this is
- How many commits are on this branch relative to main
- What files changed and in which packages
- What the apparent intent of the changes is
- Anything surprising about the scope or contents

Hold this understanding. It informs everything that follows.
