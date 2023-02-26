package utils

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrNoRecord = errors.New("record not found")
)

// String returns a pointer to the given string.
func String(v string) *string {
	return &v
}

// StringSlice returns a pointer to the give strings.
func StringSlice(v []string) *[]string {
	return &v
}

// Bool returns a pointer to the given bool
func Bool(v bool) *bool {
	return &v
}

// Int returns a pointer to the given int32.
func Int32(v int32) *int32 {
	return &v
}

// Int64 returns a pointer to the given int64.
func Int64(v int64) *int64 {
	return &v
}

func ListContentToString(listString string) string {
	re := regexp.MustCompile(`[\"\[\]]`)
	return re.ReplaceAllString(listString, "")
}

func EscapeString(in string) string {
	out := strings.ReplaceAll(in, `\`, `\\`)
	out = strings.ReplaceAll(out, `'`, `\'`)
	return out
}

func ListToSnowflakeString(list []string) string {
	for index, element := range list {
		list[index] = fmt.Sprintf(`'%v'`, strings.ReplaceAll(element, "'", "\\'"))
	}
	return fmt.Sprintf("%v", strings.Join(list, ", "))
}
