// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package log

import (
	"io"
	"os"
)

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func GetLogFile(filename string) (io.WriteCloser, error) {
	var f *os.File
	var err error
	if checkFileIsExist(filename) {
		f, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_TRUNC, 0666)

	} else {
		f, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	}
	return f, err
}
