package model

type CephInfo struct {
	CephEndPoint      string
	AK        		  string
	SK                string
}

type BucketStats struct {
	Bucket            string `json:"bucket"`
	NumShards         int    `json:"num_shards"`
	Tenant            string `json:"tenant"`
	Zonegroup         string `json:"zonegroup"`
	PlacementRule     string `json:"placement_rule"`
	ExplicitPlacement struct {
		DataPool      string `json:"data_pool"`
		DataExtraPool string `json:"data_extra_pool"`
		IndexPool     string `json:"index_pool"`
	} `json:"explicit_placement"`
	ID        string `json:"id"`
	Marker    string `json:"marker"`
	IndexType string `json:"index_type"`
	Owner     string `json:"owner"`
	Ver       string `json:"ver"`
	MasterVer string `json:"master_ver"`
	Mtime     string `json:"mtime"`
	MaxMarker string `json:"max_marker"`
	Usage     struct {
		RgwMain struct {
			Size           int `json:"size"`
			SizeActual     int `json:"size_actual"`
			SizeUtilized   int `json:"size_utilized"`
			SizeKb         int `json:"size_kb"`
			SizeKbActual   int `json:"size_kb_actual"`
			SizeKbUtilized int `json:"size_kb_utilized"`
			NumObjects     int `json:"num_objects"`
		} `json:"rgw.main"`
		RgwMultimeta struct {
			Size           int `json:"size"`
			SizeActual     int `json:"size_actual"`
			SizeUtilized   int `json:"size_utilized"`
			SizeKb         int `json:"size_kb"`
			SizeKbActual   int `json:"size_kb_actual"`
			SizeKbUtilized int `json:"size_kb_utilized"`
			NumObjects     int `json:"num_objects"`
		} `json:"rgw.multimeta"`
	} `json:"usage"`
	BucketQuota struct {
		Enabled    bool `json:"enabled"`
		CheckOnRaw bool `json:"check_on_raw"`
		MaxSize    int  `json:"max_size"`
		MaxSizeKb  int  `json:"max_size_kb"`
		MaxObjects int  `json:"max_objects"`
	} `json:"bucket_quota"`
}