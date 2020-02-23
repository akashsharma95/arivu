package utils

// AssertTrue checks whether the expression is true or not.
func AssertTrue(assert bool) {
	if !assert {
		panic("Assert failed :(")
	}
}
