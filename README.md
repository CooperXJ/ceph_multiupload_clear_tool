## Ceph清理小工具  
### 功能
清理每次分段上传未及时清理掉的分段上传片段

### 为什么需要清理
如果对bucket的配额中的object_num做了限制的话，那么分段上传遗留的片段会占用该配额中object的数量  
比如有一个bucket的只允许放置7个object，那么分段上传一个大文件（假设为45M），分段上传的大小为15M，
那么就需要进行3次上传，如果在第三次上传时中断了并且后续没有及时的取消分段上传或者继续上传，那么会导致
有2个object会被占用，也就是现在最多只能上传5个object


### 用法
- 清理指定桶的未完成的分段上传  
`cephclean clean --endpoint=172.23.27.119 --port=7480 --ak=xxx --sk=xxx -bucket=xxxx`
- 清理指定桶内的指定文件的未完成的分段上传  
`cephclean clean --endpoint=172.23.27.119 --port=7480 --ak=xxx --sk=xxx -bucket=xxxx --key=xxx`


