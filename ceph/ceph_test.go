package ceph

import (
	"ceph_multiupload_clear_tool/model"
	"fmt"
	"testing"
)

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

	err := TransferBucket(source, target, "compose", "transfer")
	if err != nil {
		fmt.Printf("无法完成迁移，err=%v\n", err.Error())
	}
}
