/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package nebula

import (
	"fmt"
	"log"
)

type Logger interface {
	Info(msg string)
	Warn(msg string)
	Error(msg string)
	Fatal(msg string)
}

type DefaultLogger struct{}

func (l DefaultLogger) Info(msg string) {
	log.Print(fmt.Sprintf("[INFO] %s", msg))
}

func (l DefaultLogger) Warn(msg string) {
	log.Print(fmt.Sprintf("[WARNING] %s", msg))
}

func (l DefaultLogger) Error(msg string) {
	log.Print(fmt.Sprintf("[ERROR] %s", msg))
}

func (l DefaultLogger) Fatal(msg string) {
	log.Fatal(fmt.Sprintf("[FATAL] %s", msg))
}
