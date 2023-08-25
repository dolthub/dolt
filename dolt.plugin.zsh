if (( $+commands[dolt] )); then
  # gen-zsh writes a file to the command line, and does not output the completion script like
  # most other cli tools. For this reason, we write the file to the local directory and source it
  local completion_path=${0:h}/_dolt

  if [[ ! -e "$completion_path" ]]; then
    # creates a completion file in the current directory
    dolt gen-zsh --file="$completion_path"
  fi

  source "$completion_path"
  compdef _dolt dolt
fi
