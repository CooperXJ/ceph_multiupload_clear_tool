package ceph

import (
	"ceph_multiupload_clear_tool/model"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"math"
)

const SIZE int64 = 1024 * 1024 * 5        //15M
const PART_SIZE int64 = 1024 * 1024 * 100 //15M

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

func ListObjects(cephClient *s3.S3, bucket string) ([]*s3.Object, error) {
	inputs := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}

	objects, err := cephClient.ListObjects(inputs)
	if err != nil {
		fmt.Printf("无法%v下的找到objects,err = %v\n", bucket, err.Error())
		return nil, err
	}

	return objects.Contents, nil
}

func UploadBySize(cephClient *s3.S3, key string, size int64, bucket string, body io.ReadSeeker, ctx context.Context) error {
	if size <= SIZE {
		input := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   body,
		}
		_, err := cephClient.PutObject(input)
		if err != nil {
			fmt.Printf("%v无法上传，err=%v\n", key, err.Error())
			return err
		}
		fmt.Printf("%v上传成功\n", key)
		return nil
	} else {
		input := &s3.CreateMultipartUploadInput{
			Bucket:             aws.String(bucket),
			ContentDisposition: nil,
			Key:                aws.String(key),
			Metadata:           nil,
		}
		_, out := cephClient.CreateMultipartUploadRequest(input)

		// 划分part
		partCnt := int(math.Ceil(float64(size / PART_SIZE)))
		parts := make([]*s3.CompletedPart, partCnt)
		for i := 0; i < partCnt; i++ {
			upload := &s3.UploadPartInput{
				Body:          body,
				Bucket:        aws.String(bucket),
				ContentLength: aws.Int64(size),
				Key:           aws.String(key),
				PartNumber:    nil,
				UploadId:      out.UploadId,
			}

			partUploadRes, err := cephClient.UploadPart(upload)
			if err != nil {
				return err
			}

			parts[i] = &s3.CompletedPart{
				ETag:       partUploadRes.ETag,
				PartNumber: aws.Int64(int64(i)),
			}
		}

		uploads := &s3.CompletedMultipartUpload{Parts: parts}

		cinput := &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(key),
			MultipartUpload: uploads,
			UploadId:        nil,
		}
		_, err := cephClient.CompleteMultipartUpload(cinput)
		if err != nil {
			fmt.Printf("%v无法分段上传,err=%v\n", key, err.Error())
			abortInputs := &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: out.UploadId,
			}
			_, err := cephClient.AbortMultipartUpload(abortInputs)
			if err != nil {
				fmt.Printf("%v无法取消上传,err=%v\n", key, err.Error())
				return err
			}
			return err
		}

	loop:
		for {
			select {
			case <-ctx.Done():
				abortInputs := &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(key),
					UploadId: out.UploadId,
				}
				_, err := cephClient.AbortMultipartUpload(abortInputs)
				if err != nil {
					fmt.Printf("%v无法取消上传,err=%v\n", key, err.Error())
					return err
				}
				break loop
			}
		}
		return nil
	}
}

func GetObject(cephClient *s3.S3, key string, bucket string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	object, err := cephClient.GetObject(input)
	if err != nil {
		fmt.Printf("%v无法下载\n", key)
		return nil, err
	}

	return object.Body, nil
}

func TransferBucket(sourceCephInfo, targetCephInfo *model.CephInfo, sourceBucket, targetBucket string) error {
	sClient, err := GetCephConn(sourceCephInfo)
	if err != nil {
		fmt.Println("无法与源端建立连接")
		return err
	}

	tClient, err := GetCephConn(targetCephInfo)
	if err != nil {
		fmt.Println("无法与目标端建立连接")
		return err
	}

	sObjects, err := ListObjects(sClient, sourceBucket)
	if err != nil {
		fmt.Println("无法下载源端objects")
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	for index, obj := range sObjects {
		go func(ctx context.Context) error {
			body, err := GetObject(sClient, *obj.Key, sourceBucket)
			if err != nil {
				return err
			}

			err = UploadBySize(tClient, *obj.Key, *obj.Size, targetBucket, io.ReadSeeker(body), ctx)
			if err != nil {
				return err
			}
			return nil
		}(ctx)
	}
}
