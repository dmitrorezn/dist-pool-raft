// Code generated by "stringer -type cmd"; DO NOT EDIT.

package main

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[undefinedCmd-0]
	_ = x[addCmd-1]
	_ = x[listCmd-2]
	_ = x[flushCmd-3]
}

const _cmd_name = "undefinedCmdaddCmdlistCmdflushCmd"

var _cmd_index = [...]uint8{0, 12, 18, 25, 33}

func (i cmd) String() string {
	if i < 0 || i >= cmd(len(_cmd_index)-1) {
		return "cmd(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _cmd_name[_cmd_index[i]:_cmd_index[i+1]]
}