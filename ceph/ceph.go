package ceph

import (
	"bytes"
	"ceph_multiupload_clear_tool/model"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"math"
	"sync"
)

const SIZE int64 = 1024 * 1024 * 15      //15M
const PART_SIZE int64 = 1024 * 1024 * 10 //15M

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

func UploadBySize(cephClient *s3.S3, key string, size int64, bucket string, body []byte, ctx context.Context) error {
	if size <= SIZE {
		input := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		}
		_, err := cephClient.PutObject(input)
		if err != nil {
			fmt.Printf("%v无法上传，err=%v\n", key, err.Error())
			return err
		}
		return nil
	} else {
		syncChan := make(chan struct{}, 1)

		input := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}
		out, err := cephClient.CreateMultipartUpload(input)
		if err != nil {
			fmt.Printf("无法初始化上传，err=%v\n", err.Error())
		}

		//监听取消上传的动作
		go func() {
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
					}
					break loop
				case <-syncChan:
					break loop
				}
			}
		}()

		// 划分part
		partCnt := int(math.Ceil(float64(size / PART_SIZE)))
		parts := make([]*s3.CompletedPart, partCnt)

		var cur int64 = 0
		var partSize = PART_SIZE
		for i := 0; i < partCnt; i++ {
			if (cur + PART_SIZE) > size {
				partSize = size - cur
			}

			upload := &s3.UploadPartInput{
				Body:          bytes.NewReader(body[cur : cur+partSize]),
				Bucket:        aws.String(bucket),
				Key:           aws.String(key),
				PartNumber:    aws.Int64(int64(i) + 1),
				UploadId:      out.UploadId,
				ContentLength: aws.Int64(partSize),
			}

			partUploadRes, err := cephClient.UploadPart(upload)
			if err != nil {
				return err
			}

			parts[i] = &s3.CompletedPart{
				ETag:       partUploadRes.ETag,
				PartNumber: aws.Int64(int64(i) + 1),
			}

			cur += partSize
		}

		uploads := &s3.CompletedMultipartUpload{Parts: parts}

		cinput := &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(key),
			MultipartUpload: uploads,
			UploadId:        out.UploadId,
		}

		_, err = cephClient.CompleteMultipartUpload(cinput)
		if err != nil {
			fmt.Printf("%v无法分段上传,err =%v\n", key, err.Error())
			abortInputs := &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: out.UploadId,
			}
			_, errAbort := cephClient.AbortMultipartUpload(abortInputs)
			if errAbort != nil {
				fmt.Printf("%v无法取消上传,err =%v\n", key, errAbort.Error())
				return errAbort
			}
			return err
		}

		syncChan <- struct{}{}
		return nil
	}
}

func GetObject(cephClient *s3.S3, key string, bucket string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	object, err := cephClient.GetObject(input)
	if err != nil {
		fmt.Printf("%v无法下载\n", key)
		return nil, err
	}

	body, err := ioutil.ReadAll(object.Body)
	if err != nil {
		fmt.Printf("无法获取文件")
		return nil, err
	}
	return body, nil
}

//UploadGiantFile 多线程迁移大文件
func UploadGiantFile(cephClient *s3.S3, key string, size int64, bucket string, ctx context.Context) error {
	syncChan := make(chan struct{}, 1)

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	out, err := cephClient.CreateMultipartUpload(input)
	if err != nil {
		fmt.Printf("无法初始化上传，err=%v\n", err.Error())
	}

	//监听取消上传的动作
	go func() {
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
				}
				break loop
			case <-syncChan:
				break loop
			}
		}
	}()

	// 划分part
	partCnt := int(math.Ceil(float64(size / PART_SIZE)))
	parts := make([]*s3.CompletedPart, partCnt)

	var cur int64 = 0
	var partSize = PART_SIZE
	for i := 0; i < partCnt; i++ {
		if (cur + PART_SIZE) > size {
			partSize = size - cur
		}

		upload := &s3.UploadPartInput{
			Body:          bytes.NewReader(body[cur : cur+partSize]),
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			PartNumber:    aws.Int64(int64(i) + 1),
			UploadId:      out.UploadId,
			ContentLength: aws.Int64(partSize),
		}

		partUploadRes, err := cephClient.UploadPart(upload)
		if err != nil {
			return err
		}

		parts[i] = &s3.CompletedPart{
			ETag:       partUploadRes.ETag,
			PartNumber: aws.Int64(int64(i) + 1),
		}

		cur += partSize
	}

	uploads := &s3.CompletedMultipartUpload{Parts: parts}

	cinput := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		MultipartUpload: uploads,
		UploadId:        out.UploadId,
	}

	_, err = cephClient.CompleteMultipartUpload(cinput)
	if err != nil {
		fmt.Printf("%v无法分段上传,err =%v\n", key, err.Error())
		abortInputs := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: out.UploadId,
		}
		_, errAbort := cephClient.AbortMultipartUpload(abortInputs)
		if errAbort != nil {
			fmt.Printf("%v无法取消上传,err =%v\n", key, errAbort.Error())
			return errAbort
		}
		return err
	}

	syncChan <- struct{}{}
	return nil
}

func GetPartFile(cephClient *s3.S3, key string, bucket string, start int64, end int64) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("%v-%v", start, end)),
	}

	object, err := cephClient.GetObject(input)
	if err != nil {
		fmt.Printf("%v无法下载\n", key)
		return nil, err
	}

	body, err := ioutil.ReadAll(object.Body)
	if err != nil {
		fmt.Printf("无法获取文件")
		return nil, err
	}
	return body, nil
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

	wg := sync.WaitGroup{}
	wg.Add(len(sObjects))

	ctx, cancel := context.WithCancel(context.Background())
	for _, obj := range sObjects {
		go func(ctx context.Context, obj *s3.Object) {
			defer func() {
				wg.Done()
			}()

			err := func() error {
				fmt.Printf("%v开始迁移\n", *obj.Key)
				body, err := GetObject(sClient, *obj.Key, sourceBucket)
				if err != nil {
					return err
				}

				err = UploadBySize(tClient, *obj.Key, *obj.Size, targetBucket, body, ctx)
				if err != nil {
					return err
				}
				fmt.Printf("%v迁移完成\n", *obj.Key)
				fmt.Println("-----------------")
				return nil
			}()
			if err != nil {
				cancel()
			}
		}(ctx, obj)
	}

	wg.Wait()
	cancel()

	return nil
}
