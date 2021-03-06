package ceph

import (
	"bytes"
	"ceph_multiupload_clear_tool/model"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

const MIDDLE_FLAG = "__multipart_"
const LAST_FLAG = ".meta"

func Cmd(commandName string, params []string) (string, error) {
	cmd := exec.Command(commandName, params...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Start()
	if err != nil {
		return "", err
	}
	err = cmd.Wait()
	return out.String(), err
}

// GetBucketId 获取bucket的Id
func GetBucketId(bucket string) (string, error) {
	t := fmt.Sprintf("--bucket=%v", bucket)
	args := []string{"bucket", "stats", t}
	out, err := Cmd("radosgw-admin", args)
	if err != nil {
		return "", err
	}

	var bucketStats model.BucketStats
	err = json.Unmarshal([]byte(out), &bucketStats)
	if err != nil {
		return "", err
	}

	return bucketStats.ID, nil
}

// GetUselessUploadId 获取分段上传的废弃Id
func GetUselessUploadId(bucketName string, fileName string, pool string, days int) (map[string]string, error) {
	//如果没有传入参数则默认为default.rgw.buckets.non-ec pool
	if pool == "" {
		pool = "default.rgw.buckets.non-ec"
	}

	bucketId, err := GetBucketId(bucketName)
	if err != nil {
		fmt.Println("无法找到该bucket")
		return nil, err
	}

	args := []string{"-c", fmt.Sprintf("rados -p %s ls |grep %s", pool, bucketId)}
	out, err := Cmd("bash", args)
	if err != nil {
		return nil, err
	}

	uploads := strings.Split(out, "\n")
	uploadMap := make(map[string]string, 2)
	var formerPart string
	if fileName == "" {
		formerPart = fmt.Sprintf("%s%s", bucketId, MIDDLE_FLAG)
		for _, upload := range uploads {
			if len(upload) <= 1 {
				continue
			} else {
				uploadId := strings.Replace(strings.Replace(upload, formerPart, "", 1), LAST_FLAG, "", 1)
				index := strings.LastIndexAny(uploadId, ".")
				uploadMap[uploadId[index+1:]] = uploadId[:index]
			}
		}
	} else {
		formerPart = fmt.Sprintf("%s%s%s", bucketId, MIDDLE_FLAG, fileName)
		for _, upload := range uploads {
			if len(upload) <= 1 {
				continue
			} else {
				uploadId := strings.Replace(strings.Replace(upload, formerPart, "", 1), LAST_FLAG, "", 1)
				uploadMap[uploadId[1:]] = fileName
			}
		}
	}

	return uploadMap, nil
}
