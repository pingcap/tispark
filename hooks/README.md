## Guide to Use Git Pre-Commit Hook to format code

Inspired by [this gist script](https://gist.github.com/alodavi/c42da82b888869bf935242b1160e05b7#file-pre-commit-hook-install-sh), we use maven plugin to do the work instead of `scalafmt`


Run the following commands:
```bash
cd $ProjectRoot/hooks
./pre-commit-hook-install.sh
```

The hook will be added to local `.git` directory, and will be performed each time pre-commit.

Note that the if any changes are applied to `pre-commit-hook.sh`, the above instructions should be run again.

This pre-commit hook will force format before commit, if un-staged changes exists, the commit will abort when code is not already formatted. It is due to possible merge conflicts of stashed un-staged changes and un-staged formatted changes.
