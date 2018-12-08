package es

import (
	"time"
)

func TimeToES(t time.Time) string {
	return t.Format("2006-01-02T15:04:05+08")
}
func ParseESTime(str string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05.000Z", str)
	if err != nil {
		return time.Now()
	}
	return t
}
