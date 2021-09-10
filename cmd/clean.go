package cmd

import (
	"ceph_multiupload_clear_tool/ceph"
	"ceph_multiupload_clear_tool/model"
	"fmt"
	"github.com/spf13/cobra"
	"net"
)

var (
	cephInfo *model.CephInfo
)

func init() {
	cleanCmd.Flags().StringP("endpoint","","127.0.0.1","绑定端口")

	cleanCmd.Flags().StringP("ak","","","ak（required）")
	cleanCmd.MarkFlagRequired("ak")

	cleanCmd.Flags().StringP("sk","","","sk（required）")
	cleanCmd.MarkFlagRequired("sk")

	cleanCmd.Flags().StringP("bucket","","","bucket")
	cleanCmd.Flags().StringP("key","","","fileName")
}

var cleanCmd = &cobra.Command{
	Use:   "clean",
	Short: "清理废弃的分段上传",
	Long:  `cephtool clean --ep 127.0.0.1 --ak xxxx --sk xxxxx`,
	Run: func(cmd *cobra.Command, args []string) {
		endpoint, err := cmd.Flags().GetString("endpoint")
		if err != nil || net.ParseIP(endpoint)==nil {
			fmt.Println("请检查IP输入")
		}

		ak, err := cmd.Flags().GetString("ak")
		if err != nil {
			fmt.Println("请检查ak输入")
		}

		sk, err := cmd.Flags().GetString("sk")
		if err != nil {
			fmt.Println("请检查sk输入")
		}

		cephInfo = &model.CephInfo{
			CephEndPoint: endpoint,
			AK:           ak,
			SK:           sk,
		}

		bucket, err := cmd.Flags().GetString("bucket")
		if err != nil {
			fmt.Println("请检查bucket输入")
		}

		key, err := cmd.Flags().GetString("key")
		if err != nil {
			fmt.Println("请检查key输入")
		}


		fmt.Println(endpoint+" "+ak+" "+sk)

		conn, err := ceph.GetCephConn(*cephInfo)
		if err != nil {
			fmt.Println("无法通过验证，请检查endpoint、ak、sk")
		}

		uploadIdMap, err := ceph.GetUselessUploadId(bucket, key, "", -1)
		if err != nil {
			fmt.Println("无法找到Id")
		}

		fmt.Printf("需要清理的分段上传如下：\n %s",uploadIdMap)

		for key,uploadId:=range uploadIdMap{
			err := ceph.CancelMultiUploadByKey(bucket, key, uploadId, conn)
			if err!=nil {
				fmt.Printf("清理%s文件的上传分段%s失败",key,uploadId)
			}else{
				fmt.Printf("清理%s文件的上传分段%s成功!!!",key,uploadId)
			}
		}
	},
}

