package test

import (
	"ceph_multiupload_clear_tool/ceph"
	"ceph_multiupload_clear_tool/model"
	"fmt"
	"testing"
)

//func minArray(numbers []int) int {
//	low := 0
//	high := len(numbers) - 1
//	for low < high {
//		mid := (high-low)/2 + low
//		if numbers[mid] > numbers[high] {
//			low = mid + 1
//		} else if numbers[mid] < numbers[high] {
//			high = mid
//		} else {
//			high -= 1
//		}
//	}
//
//	return numbers[low]
//}
//
//func Test1(t *testing.T) {
//	fmt.Println(minArray([]int{1, 2, 2}))
//}

func TestTransferBucket(t *testing.T) {
	source := &model.CephInfo{
		CephEndPoint: "172.23.27.119",
		Port:         "7480",
		AK:           "4S897Y9XN9DBR27LAI1L",
		SK:           "WmZ6JRoMNxmtE9WtXM9Jrz8BhEdZnwzzAYcE6b1z",
	}

	target := &model.CephInfo{
		CephEndPoint: "172.23.27.119",
		Port:         "7480",
		AK:           "4S897Y9XN9DBR27LAI1L",
		SK:           "WmZ6JRoMNxmtE9WtXM9Jrz8BhEdZnwzzAYcE6b1z",
	}

	err := ceph.TransferBucket(source, target, "compose", "transfer")
	if err != nil {
		fmt.Printf("无法完成迁移，err=%v\n", err.Error())
	}
}
