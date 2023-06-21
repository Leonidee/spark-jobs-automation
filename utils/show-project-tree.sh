#!/usr/bin/env bash
#
# Shows project tree with exa
#
exa --tree --all --git-ignore --ignore-glob="images|.git*|LICENSE|*.lock|*.toml|docker-compose.yaml|*.md|*init*"