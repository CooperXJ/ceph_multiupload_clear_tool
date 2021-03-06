package cmd

import (
	"ceph_multiupload_clear_tool/ceph"
	"ceph_multiupload_clear_tool/model"
	"fmt"
	"github.com/spf13/cobra"
	"net"
)

func init() {
	cleanCmd.Flags().StringP("endpoint", "", "127.0.0.1", "rgw地址")
	cleanCmd.Flags().StringP("port", "p", "7480", "rgw端口")

	cleanCmd.Flags().StringP("ak", "", "", "ak（required）")
	cleanCmd.MarkFlagRequired("ak")

	cleanCmd.Flags().StringP("sk", "", "", "sk（required）")
	cleanCmd.MarkFlagRequired("sk")

	cleanCmd.Flags().StringP("bucket", "", "", "bucket（required）")

	cleanCmd.Flags().StringP("key", "", "", "fileName")

	cleanCmd.Flags().StringP("pool", "", "", "rgw pool")
}

var cleanCmd = &cobra.Command{
	Use:   "clean",
	Short: "清理废弃的分段上传",
	Long:  `cephtool clean --ep 127.0.0.1 --ak xxxx --sk xxxxx`,
	Run: func(cmd *cobra.Command, args []string) {
		// 捕获异常
		defer func() {
			if r := recover(); r != nil {
			}
		}()

		endpoint, err := cmd.Flags().GetString("endpoint")
		if err != nil || net.ParseIP(endpoint) == nil {
			fmt.Println("请检查IP输入")
			return
		}

		ak, err := cmd.Flags().GetString("ak")
		if err != nil {
			fmt.Println("请检查ak输入")
			return
		}

		sk, err := cmd.Flags().GetString("sk")
		if err != nil {
			fmt.Println("请检查sk输入")
			return
		}

		key, err := cmd.Flags().GetString("key")
		if err != nil {
			fmt.Println("请检查key输入")
			return
		}

		port, err := cmd.Flags().GetString("port")
		if err != nil {
			fmt.Println("请检查输入的port")
			return
		}

		pool, err := cmd.Flags().GetString("pool")
		if err != nil {
			fmt.Println("请检查输入的port")
			return
		}

		cephInfo := &model.CephInfo{
			CephEndPoint: endpoint,
			Port:         port,
			AK:           ak,
			SK:           sk,
		}

		bucket, err := cmd.Flags().GetString("bucket")
		if err != nil {
			fmt.Println("请检查bucket输入")
			return
		}

		uploadIdMap := make(map[string]string, 2)
		conn, err := ceph.GetCephConn(cephInfo)
		if err != nil || ceph.TestIfValid(bucket, conn) != nil {
			fmt.Println("无法通过验证，请检查endpoint、ak、sk或者bucket不属于该用户")
			fmt.Println(err.Error())
			return
		}

		if bucket == "" {
			bucketNameList, err := ceph.GetAllBucket(conn)
			if err != nil {
				fmt.Println(err.Error())
			}

			for _, bucket := range bucketNameList {
				uploadIdMap, err = ceph.GetUselessUploadId(bucket, key, pool, -1)
				if err != nil {
					fmt.Printf("无法找到桶%v相关分段上传Id\n", bucket)
				} else {
					fmt.Printf("桶%v需要清理的分段上传如下：\n", bucket)
					for k, v := range uploadIdMap {
						fmt.Printf("%s=>%s\n", k, v)
					}

					for uploadId, key := range uploadIdMap {
						err := ceph.CancelMultiUploadByKey(bucket, key, uploadId, conn)
						if err != nil {
							fmt.Printf("清理%s文件的分段上传%s失败\n", key, uploadId)
							fmt.Println(err.Error())
						} else {
							fmt.Printf("清理%s文件的分段上传%s成功!!!\n", key, uploadId)
						}
					}
				}
			}
		} else {
			uploadIdMap, err = ceph.GetUselessUploadId(bucket, key, pool, -1)
			if err != nil {
				fmt.Println("无法找到该桶内相关分段上传Id")
				return
			}

			fmt.Printf("需要清理的分段上传如下：\n")
			for k, v := range uploadIdMap {
				fmt.Printf("%s=>%s\n", k, v)
			}

			for uploadId, key := range uploadIdMap {
				err := ceph.CancelMultiUploadByKey(bucket, key, uploadId, conn)
				if err != nil {
					fmt.Printf("清理%s文件的分段上传%s失败\n", key, uploadId)
					fmt.Println(err.Error())
				} else {
					fmt.Printf("清理%s文件的分段上传%s成功!!!\n", key, uploadId)
				}
			}
		}
	},
}
