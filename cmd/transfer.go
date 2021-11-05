package cmd

import (
	"ceph_multiupload_clear_tool/ceph"
	"ceph_multiupload_clear_tool/model"
	"fmt"
	"github.com/spf13/cobra"
	"net"
)

func init() {
	cleanCmd.Flags().StringP("source", "s", "127.0.0.1", "rgw地址")
	cleanCmd.Flags().StringP("source_port", "sp", "7480", "rgw端口")

	cleanCmd.Flags().StringP("target", "t", "127.0.0.1", "rgw地址")
	cleanCmd.Flags().StringP("target_port", "tp", "7480", "rgw端口")

	cleanCmd.Flags().StringP("source_ak", "", "", "源ak（required）")
	cleanCmd.MarkFlagRequired("source_ak")

	cleanCmd.Flags().StringP("source_sk", "", "", "源sk（required）")
	cleanCmd.MarkFlagRequired("source_sk")

	cleanCmd.Flags().StringP("target_ak", "", "", "目标ak（required）")
	cleanCmd.MarkFlagRequired("target_ak")

	cleanCmd.Flags().StringP("target_sk", "", "", "目标sk（required）")
	cleanCmd.MarkFlagRequired("target_sk")

	cleanCmd.Flags().StringP("source_bucket", "sb", "", "源桶（required）")
	cleanCmd.Flags().StringP("target_bucket", "tb", "", "目标桶")
}

var transferCmd = &cobra.Command{
	Use:   "transfer",
	Short: "迁移bucket数据",
	Long:  `cephtool transfer --ak xxxx --sk xxxxx --source 172.23.27.115 --source_port 7480 --source_bucket test --target 172.23.27.120 --target_port 7480 --target_bucket test`,
	Run: func(cmd *cobra.Command, args []string) {
		// 捕获异常
		defer func() {
			if r := recover(); r != nil {
			}
		}()

		source, err := cmd.Flags().GetString("source")
		if err != nil || net.ParseIP(source) == nil {
			fmt.Println("请检查source输入")
			return
		}

		target, err := cmd.Flags().GetString("target")
		if err != nil || net.ParseIP(target) == nil {
			fmt.Println("请检查target输入")
			return
		}

		source_port, err := cmd.Flags().GetString("source_port")
		if err != nil {
			fmt.Println("请检查source_port输入")
			return
		}

		target_port, err := cmd.Flags().GetString("target_port")
		if err != nil {
			fmt.Println("请检查target_port输入")
			return
		}

		source_ak, err := cmd.Flags().GetString("source_ak")
		if err != nil {
			fmt.Println("请检查source_ak输入")
			return
		}

		source_sk, err := cmd.Flags().GetString("source_sk")
		if err != nil {
			fmt.Println("请检查source_sk输入")
			return
		}

		target_ak, err := cmd.Flags().GetString("target_ak")
		if err != nil {
			fmt.Println("请检查target_ak输入")
			return
		}

		target_sk, err := cmd.Flags().GetString("target_sk")
		if err != nil {
			fmt.Println("请检查target_sk输入")
			return
		}

		source_bucket, err := cmd.Flags().GetString("source_bucket")
		if err != nil {
			fmt.Println("请检查source_bucket输入")
			return
		}

		target_bucket, err := cmd.Flags().GetString("target_bucket")
		if err != nil {
			fmt.Println("请检查target_bucket输入")
			return
		}

		sourceCeph := &model.CephInfo{
			CephEndPoint: source,
			Port:         source_port,
			AK:           source_ak,
			SK:           source_sk,
		}

		targetCeph := &model.CephInfo{
			CephEndPoint: target,
			Port:         target_port,
			AK:           target_ak,
			SK:           target_sk,
		}

		err = ceph.TransferBucket(sourceCeph, targetCeph, source_bucket, target_bucket)
		if err != nil {
			fmt.Printf("无法完成迁移,%v\n", err.Error())
			return
		}
	},
}
