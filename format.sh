#!/bin/bash

format_cmd="clang-format -style=file -i '{}'"
source_regex="^(src).*\.(cc|cpp|h|y)"

find src | grep -E "$source_regex" | xargs -I{} sh -c "${format_cmd}"
