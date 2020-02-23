package utils

import "strings"

// PathivuConfigPrefix is the prefix of the pathivu instance config key.
const PathivuConfigPrefix = "PATHIVU"

// DELIMITOR to seperate prefix and tail
const DELIMITOR = "/"

// DestinationKey will give the destination key for the given source.
func DestinationKey(source string) string {
	return "DEST" + DELIMITOR + source
}

// PathivuConfigKey gives pathivu config key for given uuid.
func PathivuConfigKey(uuid string) string {
	return PathivuConfigPrefix + DELIMITOR + uuid
}

// UUIDromConfigKey will get uuid from the pathivu config key.
func UUIDromConfigKey(configKey string) string {
	splits := strings.Split(configKey, DELIMITOR)
	AssertTrue(len(splits) == 2)
	AssertTrue(splits[0] == PathivuConfigPrefix)
	return splits[1]
}
