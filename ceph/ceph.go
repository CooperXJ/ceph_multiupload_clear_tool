package ceph

import (
	"ceph_multiupload_clear_tool/model"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

//GetCephConn 获取连接
func GetCephConn(cephInfo *model.CephInfo) (*s3.S3, error) {

	//初始化ceph的一些信息
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(cephInfo.AK, cephInfo.SK, ""),
		Endpoint:         aws.String(fmt.Sprintf("http://%s:%s", cephInfo.CephEndPoint, cephInfo.Port)),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true), //virtual-host style方式，不要修改
	})

	if err != nil {
		return nil, err
	}

	//创建s3类型的连接
	return s3.New(sess), nil
}

//CancelMultiUploadByKey 根据key删除无用的多段上传片段
func CancelMultiUploadByKey(bucket string, key string, uploadId string, cephClient *s3.S3) error {
	params := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	}

	_, err := cephClient.AbortMultipartUpload(params)
	if err != nil {
		return err
	}

	return nil
}

// TestIfValid 通过list bucket操作查看是否账号密码正确以及当前用户是否包含该bucket
func TestIfValid(bucket string, cephClient *s3.S3) error {
	out, err := cephClient.ListBuckets(nil)
	if err != nil {
		return errors.New("内部错误")
	}

	if bucket != "" {
		for _, temp := range out.Buckets {
			if (*temp.Name) == bucket {
				return nil
			}
		}
	} else {
		bucketList := make([]string, 0)
		for _, temp := range out.Buckets {
			bucketList = append(bucketList, *temp.Name)
		}
		if len(bucketList) > 0 {
			return nil
		}
	}

	return errors.New("账号密码错误或不存在该bucket")
}

// GetAllBucket 获取用户的所有bucket
func GetAllBucket(cephClient *s3.S3) ([]string, error) {
	out, err := cephClient.ListBuckets(nil)
	if err != nil {
		return nil, errors.New("账号密码错误")
	}

	bucketList := make([]string, 0)
	for _, temp := range out.Buckets {
		bucketList = append(bucketList, *temp.Name)
	}

	return bucketList, nil
}
