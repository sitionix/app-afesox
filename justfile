generate-api:
    #!/usr/bin/env bash
    name=$(grep '^\s*- name:' apis/metadata.yml | sed 's/.*- name: //' | fzf)
    gh pr comment --body "/generate --name \"${name}\""