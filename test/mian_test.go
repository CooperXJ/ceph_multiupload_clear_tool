package test

import (
	"fmt"
	"testing"
)

func minArray(numbers []int) int {
	low := 0
	high := len(numbers) - 1
	for low < high {
		mid := (high-low)/2 + low
		if numbers[mid] > numbers[high] {
			low = mid + 1
		} else if numbers[mid] < numbers[high] {
			high = mid
		} else {
			high -= 1
		}
	}

	return numbers[low]
}

func Test1(t *testing.T) {
	fmt.Println(minArray([]int{1, 2, 2}))
}
