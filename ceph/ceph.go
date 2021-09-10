package ceph

import (
	"ceph_multiupload_clear_tool/model"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

//GetCephConn 获取连接
func GetCephConn(cephInfo model.CephInfo) (*s3.S3, error) {

	//初始化ceph的一些信息
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(cephInfo.AK, cephInfo.SK, ""),
		Endpoint:         aws.String(cephInfo.CephEndPoint),
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
func CancelMultiUploadByKey(bucket string,key string, uploadId string,cephClient *s3.S3) error {
	params := &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key: aws.String(key),
		UploadId: aws.String(uploadId),
	}

	_, err := cephClient.AbortMultipartUpload(params)
	if err != nil {
		return err
	}

	return nil
}

// TestIfValid 通过list bucket操作查看是否账号密码正确
func TestIfValid(cephClient *s3.S3) error{
	params:= &s3.ListBucketsInput{}
	_, err := cephClient.ListBuckets(params)

	if err != nil {
		return err
	}

	return nil
}

