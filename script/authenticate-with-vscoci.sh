#!/bin/bash

test "Darwin" = "$(uname)" && exit 1

NETRC_FILE="$HOME/.netrc"

if test "$1" = "remove"; then
  echo "INFO: removing $NETRC_FILE"
  rm $NETRC_FILE
else
  echo "INFO: generating $NETRC_FILE"
  cat > $NETRC_FILE << EOF
machine github.com
  login vscoci
  password c00886b466232235da3eb598fd35a78997d1ada1
EOF
  chmod 600 $NETRC_FILE
  echo "$0 finished."
fi
