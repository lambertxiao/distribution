package us3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	ufsdk "github.com/ufilesdk-dev/ufile-gosdk"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
)

const driverName = "us3"

const apiName = "api.ucloud.cn"

// listMax is the largest amount of objects you can request from S3 in a list call
const listMax = 1000

const limit = 100

const maxParts = 1000

var BlkSize = 0 // 保存 InitiateMultipartUpload 返回的分片大小

type DriverParameters struct {
	PublicKey       string // *************
	PrivateKey      string // *************
	Api             string // api.ucloud.cn
	Bucket          string // test-hzy
	Regin           string // cn-sh2
	Endpoint        string // cn-sh2.ufileos.com
	VerifyUploadMD5 bool   // false
	RootDirectory   string // /my_images 或 /
}

// ============================= init ===================================

func init() {
	factory.Register(driverName, &us3DriverFactory{})
}

// ============================ factory =================================

type us3DriverFactory struct{}

// Implement factory.StorageDriverFactory interface
func (factory *us3DriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// ============================= dirver =================================

type driver struct {
	// TODO(zengyan) driver struct 哪些是用不到的数据
	Req             *ufsdk.UFileRequest
	PublicKey       string
	PrivateKey      string
	Api             string
	Bucket          string
	Endpoint        string
	VerifyUploadMD5 bool
	RootDirectory   string
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	publicKey, ok := parameters["PublicKey"]
	if !ok {
		return nil, fmt.Errorf("No PublicKey parameter provided")
	}
	privateKey, ok := parameters["PrivateKey"]
	if !ok {
		return nil, fmt.Errorf("No PrivateKey parameter provided")
	}
	api, ok := parameters["Api"]
	if !ok {
		api = apiName
	}
	bucket, ok := parameters["Bucket"]
	if !ok {
		return nil, fmt.Errorf("No Bucket parameter provided")
	}
	regin, ok := parameters["Regin"]
	if !ok {
		return nil, fmt.Errorf("No Regin parameter provided")
	}
	endpoint, ok := parameters["Endpoint"]
	if !ok {
		return nil, fmt.Errorf("No Endpoint parameter provided")
	}
	verifyUploadMD5 := false
	verifyUploadMD5Bool, ok := parameters["VerifyUploadMD5"]
	if ok {
		verifyUploadMD5, ok = verifyUploadMD5Bool.(bool)
		if !ok {
			return nil, fmt.Errorf("VerifyUploadMD5 parameter is not a type of 'bool'")
		}
	}
	rootDirectory, ok := parameters["RootDirectory"]
	if !ok {
		rootDirectory = ""
	}

	param := DriverParameters{
		PublicKey:       fmt.Sprint(publicKey),
		PrivateKey:      fmt.Sprint(privateKey),
		Api:             fmt.Sprint(api),
		Bucket:          fmt.Sprint(bucket),
		Regin:           fmt.Sprint(regin),
		Endpoint:        fmt.Sprint(endpoint),
		VerifyUploadMD5: verifyUploadMD5,
		RootDirectory:   fmt.Sprint(rootDirectory),
	}
	return New(param)
}

func New(params DriverParameters) (*Driver, error) {
	// 以下用 new 或 := 都可以
	// config := new(ufsdk.Config)
	// config.PublicKey = params.PublicKey
	// config.PrivateKey = params.PrivateKey
	// config.BucketHost = params.Api
	// config.BucketName = params.Bucket
	// config.FileHost = params.Endpoint
	// config.VerifyUploadMD5 = params.VerifyUploadMD5
	// config.Endpoint = ""
	// logrus.Info(">>> New()")
	// logrus.Info(">>> config is ", config)

	config := &ufsdk.Config{
		PublicKey:       params.PublicKey,
		PrivateKey:      params.PrivateKey,
		BucketHost:      params.Api,
		BucketName:      params.Bucket,
		FileHost:        params.Endpoint,
		VerifyUploadMD5: params.VerifyUploadMD5,
		Endpoint:        "",
	}

	req, err := ufsdk.NewFileRequest(config, nil)
	if err != nil {
		return nil, err
	}

	// Validate that the given credentials have at least read permissions in the
	// given bucket scope.
	if _, err := req.ListObjects(strings.TrimRight(params.RootDirectory, "/"), "", "", 1); err != nil {
		return nil, err
	}

	d := &driver{
		Req:             req,
		PublicKey:       params.PublicKey,
		PrivateKey:      params.PrivateKey,
		Api:             params.Api,
		Bucket:          params.Bucket,
		Endpoint:        params.Endpoint,
		VerifyUploadMD5: params.VerifyUploadMD5,
		RootDirectory:   params.RootDirectory,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface
func (d *driver) Name() string {
	return driverName
}

// 下载 path 所对应的 image
// 注：具体的 bucket 信息存放在 d 中
// 注：path 为 image 的名字
// 如：/hello-world，那么该文件的 key 就应该为 /my_images/hello-world
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	// logrus.Infof(">> GetContent()")
	// fileInfo, err := d.Stat(ctx, path)
	// if fileInfo != nil && err == nil && fileInfo.IsDir() { // d.us3Path(path) 是一个目录，无法 get
	// 	// return nil, fmt.Errorf("Path is a dir existed, but GetContent() can not support to get dir. Or path is a normal file, but the file isn't existed.\n  path is %s\n  full path is %s\n", path, d.us3Path(path))
	// 	return nil, storagedriver.PathNotFoundError{Path: path}
	// } else if fileInfo == nil && err != nil { // 根本不存在 d.us3Path(path) 这个文件
	// 	statErr, ok := err.(storagedriver.PathNotFoundError)
	// 	if ok {
	// 		return nil, statErr
	// 	}
	// }

	data, err := d.getContent(d.us3Path(path), 0)
	if err != nil {
		// return nil, err // 注：这里一定不会返回 storagedriver.PathNotFoundError，因为上面的 Stat() 已经做了判断
		return nil, parseError(path, d.Req.ParseError())
	}
	return data, nil
}

// 上传 path 所对应的 image
// 注：具体的 bucket 信息存放在 d 中
// 注：path 为 image 的名字
// 如：/hello-world，那么该文件的 key 就应该为 /my_images/hello-world
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	// logrus.Infof(">> PutContent()")
	if len(contents) >= 4*1024*1024 { // contents >= 4M 采用分片流式上传
		return d.Req.IOMutipartAsyncUpload(bytes.NewReader(contents), d.us3Path(path), d.getContentType())
	} else { // contents < 4M 采用普通流式上传
		return d.Req.IOPut(bytes.NewReader(contents), d.us3Path(path), d.getContentType())
	}
}

// 从 offset 号字节处开始读取 d.us3Path(path) 所对应的文件
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	// logrus.Infof(">> Reader()")
	// logrus.Infof(">> Reader()\n\t>>> offset = %d\n", offset)
	// fileInfo, err := d.Stat(ctx, path)
	// if fileInfo != nil && err == nil && fileInfo.IsDir() { // d.us3Path(path) 是一个目录，无法 get
	// 	// return nil, fmt.Errorf("Path is a dir existed, but GetContent() can not support to get dir. Or path is a normal file, but the file isn't existed.\n  path is %s\n  full path is %s\n", path, d.us3Path(path))
	// 	return nil, storagedriver.PathNotFoundError{Path: path}
	// } else if fileInfo == nil && err != nil { // 根本不存在 d.us3Path(path) 这个文件
	// 	statErr, ok := err.(storagedriver.PathNotFoundError)
	// 	if ok {
	// 		return nil, statErr
	// 	}
	// }

	respBody, err := d.Req.DownloadFileRetRespBody(d.us3Path(path), offset)
	if err != nil {
		err = d.Req.ParseError()
		if us3Err, ok := err.(*ufsdk.Error); ok && us3Err.StatusCode == http.StatusRequestedRangeNotSatisfiable && (us3Err.ErrMsg == "invalid range" || us3Err.RetCode == 0) {
			// return ioutil.NopCloser(bytes.NewReader(nil)), storagedriver.InvalidOffsetError{Path: path, Offset: offset}
			return ioutil.NopCloser(bytes.NewReader(nil)), nil // 如果发生 range 非法，则需要返回一个空的 reader，且不要返回 err。这是 distribution 要求的，不然无法通过它的测试用例！！！
		}
		// return nil, err // 注：这里一定不会返回 storagedriver.PathNotFoundError，因为上面的 Stat() 已经做了判断
		return nil, parseError(path, err)
	}
	return respBody, nil
}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	// logrus.Infof(">>> Writer()")
	key := d.us3Path(path)
	if !append {
		state, err := d.Req.InitiateMultipartUpload(key, d.getContentType())
		if err != nil {
			return nil, err
		}
		// 保存分片大小
		BlkSize = state.BlkSize
		return d.newWriter(key, state, nil), nil
	}
	list, err := d.Req.GetMultiUploadId(key, "", "", limit) // 获取当前 bucket 正在进行分片上传的，但未 finish 的 upload 事件（即所有 multiState）
	if err != nil {
		return nil, parseError(path, d.Req.ParseError()) // TODO(zengyan) 不确定这个 API 返回的 ErrMsg
	}
	for _, dataSet := range list.DataSet {
		// TODO(zengyan) 这里用 keyname 来标识一个文件，那么存在一种 bug
		// 猜测解决方案：
		//  1. 在 write 中，每次出现分块上传出错的时候，执行 abort
		//  2. 保证用户使用分块上传时，上传成功或失败后一定要分别调用 Commit 和 Cancel
		// op1：append 分块上传 key-A，uploadId-A。但因为某些原因上传失败，应该上传 4 块分片，却仅上传了 0 块分片
		// op2：重新 append 分块上传 key-A，uploadId-B。尝试上传第一部分，并成功上传第一部分的 4 块分片（假设一共有三部分，每部分 4 块分片）
		// op2：重新 append 分块上传 key-A，uploadId-C。尝试上传第二部分，此时程序会执行到这儿。。。
		// 	1. list = <key-A, uploadId-A>, <key-A, uploadId-B>
		//  2. 若选择 <key-A, uploadId-A> 进行 GetMultiUploadPart。则会返回 parts 为空，若在此 parts 之后继续上传，则最终文件不完整，出错。。。
		//  3. 而正确情况是希望选择 <key-A, uploadId-B> 进行 GetMultiUploadPart，返回 parts 包含 4 块分片信息
		if key != dataSet.FileName {
			continue
		}
		// 此时 state 为之前进行过分块上传 key 的那个 state，它也正是要传给 newWriter 的
		parts, err := d.Req.GetMultiUploadPart(dataSet.UploadId, maxParts, 0) // 获取当前这个 uploadId 已上传的所有 part 信息
		if err != nil {
			return nil, parseError(path, d.Req.ParseError()) // TODO(zengyan) 不确定这个 API 返回的 ErrMsg
		}
		// logrus.Infof(">>> Writer()\n\t >>> finish InitiateMultipartUpload()")
		if err != nil {
			return nil, err
		}
		state := new(ufsdk.MultipartState)
		state.GenerateMultipartState(BlkSize, dataSet.UploadId, d.getContentType(), key, parts)
		// logrus.Infof(">>> Writer()\n\t >>> finish GenerateMultipartState()")
		// logrus.Infof(">>> Writer()\n\t >>> finish AbortMultipartUpload()")
		return d.newWriter(key, state, parts), nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// 获取指定 d.us3Path(path) 的文件信息，包含其文件大小，最后一次修改时间，通过返回值返回
// 注：d.us3Path(path) 应为完整的文件名，若为普通文件则结尾没有 /，若为目录文件则结尾有 /
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	list, err := d.Req.ListObjects(d.us3Path(path), "", "", 1) // 返回包含 prefix 的所有文件，包括文件夹
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(list.Contents) == 1 {
		// 由于 ListObjects 是前缀匹配，存在一种情况，path 为真实存在的文件名，但是用户输入 path 时末尾漏了一位。这将导致程序返回 path is a dir
		// 猜测：Stat 的调用者会根据返回值进行判断，若 path is dir 则直接返回不存在 path 这个文件，因此不必担心是否将非 path 的 file 错误的视为一个 dir
		if list.Contents[0].Key != d.us3Path(path) {
			fi.IsDir = true
			// return nil, storagedriver.PathNotFoundError{Path: path}
		} else {
			fi.IsDir = false
			size, err := strconv.ParseInt(list.Contents[0].Size, 10, 64)
			if err != nil {
				return nil, err
			}
			fi.Size = size
			timestamp := time.Unix(int64(list.Contents[0].LastModified), 0)
			fi.ModTime = timestamp
		}
	} else if len(list.CommonPrefixes) == 1 {
		fi.IsDir = true // 一定走不到这儿！！！因为当 ListObjects 的 delimer 为 "" 时无论如何 resp 中都没有 CommonPrefixes 字段
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// 获取 d.us3Path(path) 目录中的文件列表
// 注：只会获取 path 中的文件（不包括目录），而不会获取子文件夹中的文件
// 注：path 末尾不能含 /，开头必须有 /，否则报错，这是 List() 上层要求的
// 注：无论 d.us3Path(path) 是否为目录文件，List() 均会将其视为目录文件，即在 d.us3Path(path) 末尾添加 /，这会保证在调用 ListObjects 时一定以 / 结尾
// 注：d.Req.ListObjects("A/B/", "", "/", listMax) 并不会返回 "A/B" 这个目录文件
// 注：若 d.us3Path(path) 目录中没有文件，则会返回一个 storagedriver.PathNotFoundError
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := opath
	// 保证 path 以 / 结尾
	if path != "/" && opath[len(path)-1] != '/' {
		path = path + "/"
	}

	// 保证 prefix 非空
	prefix := ""
	if d.us3Path("") == "" {
		prefix = "/"
	}

	us3Path := d.us3Path(path)
	// logrus.Infof(">>> prefix is %v", us3Path)
	listResponse, err := d.Req.ListObjects(us3Path, "", "/", listMax)
	if err != nil {
		return nil, parseError(path, d.Req.ParseError())
	}
	// logrus.Infof(">>> listResponse is %v", listResponse)

	files := []string{}
	directories := []string{}

	for {
		for _, key := range listResponse.Contents {
			files = append(files, strings.Replace(key.Key, d.us3Path(""), prefix, 1))
		}

		for _, commonPrefix := range listResponse.CommonPrefixes {
			commonPrefix := commonPrefix.Prefix
			directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-1], d.us3Path(""), prefix, 1))
		}

		if listResponse.IsTruncated {
			listResponse, err = d.Req.ListObjects(us3Path, listResponse.NextMarker, "/", listMax)
			if err != nil {
				return nil, parseError(path, d.Req.ParseError())
			}
		} else {
			break
		}
	}

	// This is to cover for the cases when the first key equal to us3Path.
	if len(files) > 0 && files[0] == strings.Replace(us3Path, d.us3Path(""), prefix, 1) {
		files = files[1:]
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			// Treat empty response as missing directory, since we don't actually have directories in us3.
			return nil, storagedriver.PathNotFoundError{Path: opath}
		}
	}

	return append(files, directories...), nil
}

// 将 sourcePath 对应的文件移动至 destPath 对应的文件
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	// logrus.Infof(">> Move()")
	// logrus.Infof(">>> d.Bucket is %v", d.Bucket)
	err := d.Req.Copy(d.us3Path(destPath), d.Bucket, d.us3Path(sourcePath))
	// logrus.Infof(">>> Copy's return is %v", err)
	if err != nil {
		return parseError(sourcePath, d.Req.ParseError()) // TODO(zengyan) 到底是因为 sourcePath 还是 destPath 返回的 404？？？
	}
	return d.Delete(ctx, sourcePath)
}

// 注：上层应该保证 path 一定以 / 开头，若非根路径，则不已 / 结尾
// 例如：path = /ABC target = /ABCabc 则返回 false
// 例如：path = /ABC target = /ABCabc/XYZ 则返回 false
// 例如：path = /ABC target = /ABC/abc 则返回 true
// 例如：path = /ABC target = /ABC 则返回 true
func isSubPath(path, target string) (bool, error) {
	// logrus.Infof(">>> isSubPath()\n\t>>> path is %s, target is %s\n", path, target)
	if len(path) > len(target) {
		return false, fmt.Errorf("Something error, path should be prefix of target. While path is %s, target is %s\n", path, target)
	} else if len(path) == len(target) || target[len(path)] == '/' {
		return true, nil
	}
	return false, nil
}

// 删除 path 下的所有文件（包括目录文件）
// 注：上层保证 path 一定以 / 开头，若非根路径，则不已 / 结尾
// 注：path 是一个路径，它可以表示一个文件，也可以表示一个目录
func (d *driver) Delete(ctx context.Context, path string) error {
	// logrus.Infof(">>> Delete()")
	_, statErr := d.Stat(ctx, path)
	if err, ok := statErr.(storagedriver.PathNotFoundError); ok {
		return err
	}
	prefix := d.us3Path(path)
	marker := ""
	for {
		// logrus.Infof(">>> Delete()\n\t>>> prefix is %v", prefix)
		list, err := d.Req.ListObjects(prefix, marker, "", listMax)
		// logrus.Infof(">>> Delete()\n\t>>> list is %v", list)
		if err != nil {
			return parseError(path, d.Req.ParseError())
		}
		for _, object := range list.Contents {
			ok, err := isSubPath(prefix, object.Key)
			if err != nil {
				return err
			}
			if ok { // 仅当 object.Key 等于 prefix 或是 prefix 的子目录时才会删除该 object。否则应该跳过该 object
				err = d.Req.DeleteFile(object.Key)
				if err != nil {
					return parseError(path, d.Req.ParseError())
				}
			}
		}
		// logrus.Infof(">>> Delete()\n\t>>> marker is %v, NextMarker is %v", marker, list.NextMarker)
		marker = list.NextMarker
		// logrus.Infof(">>> Delete()\n\t>>> marker is %v, NextMarker is %v", marker, list.NextMarker)

		if len(list.Contents) == 0 || len(list.NextMarker) <= 0 {
			break
		}
	}
	return nil
}

// 获取 key 等于 d.us3Path(path) 的文件 url
// options 包含 expiry，用于设置该 url 的有效时间
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != "GET") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresIn := 20 * time.Minute // 默认该 URL 有 20 分钟的有效时间
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresIn = time.Until(et)
		}
	}

	return d.Req.GetPrivateURL(d.us3Path(path), expiresIn), nil
}

// 遍历 path 路径下所有的文件，并对每个文件调用 f
// 注：path 应该是一个目录文件，并且 path 不能以 / 结尾
// 问题在两点：1.Walk的真实需求，2.List的真实需求
// TODO(zengyan) Walk() 目前的效果：递归遍历 d.us3Path(path) 这个目录中的所有文件，先遍历目录文件。若遇到空的目录文件，则会直接退出，不会继续遍历
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	// 注：storagedriver.WalkFallback 里会先调用 List
	return storagedriver.WalkFallback(ctx, d, path, f)
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

// 将 d.RootDirectory 与 path 拼起来
// 用于获取 path 对应文件的完整 key
// 猜测：path 为 image 的文件名
// 若要 put 的 image 为 106.75.215.32:8080/library/hello-world，则 path 为 /library/hello-world
// 调用 us3Path 后返回 d.RootDirectory+path。即：/my_images/library/hello-world 或 /library/hello-world
func (d *driver) us3Path(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.RootDirectory, "/")+path, "/")
}

func (d *driver) getContent(key string, offset int64) ([]byte, error) {
	// 注：key 为绝对路径
	// 默认采取流式下载
	respBody, err := d.Req.DownloadFileRetRespBody(key, offset)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(respBody)
	respBody.Close()
	return data, err
}

func parseError(path string, err error) error {
	// fmt.Printf(">>> parseError()\n\t>>> %v\n", err)
	if us3Err, ok := err.(*ufsdk.Error); ok && us3Err.StatusCode == http.StatusNotFound && (us3Err.ErrMsg == "file not exist" || us3Err.RetCode == 0) {
		return storagedriver.PathNotFoundError{Path: path}
	}
	return err
}

// ============================= writer ==================================

var ( // 暂存还未上传的 part
	ReadyPart   []byte
	PendingPart []byte
)

type writer struct {
	driver      *driver
	state       *ufsdk.MultipartState
	parts       []*ufsdk.Part
	key         string // 注：key 为完整的绝对路径，即 d.us3Path(path)
	size        int64
	readyPart   []byte
	pendingPart []byte
	closed      bool
	committed   bool
	cancelled   bool
}

func (d *driver) newWriter(key string, state *ufsdk.MultipartState, parts []*ufsdk.Part) storagedriver.FileWriter {
	// logrus.Infof(">>> newWriter()")
	var size int64
	for _, part := range parts {
		size += int64(part.Size)
	}
	// 此时 size 不等于 Write 返回的值，这会无法通过 c.Assert(writer.Size(), check.Equals, curSize) 这个测试
	// 因此需要更新 size，让其加上此时还未上传的 part 的 size
	size += int64(len(ReadyPart))
	return &writer{
		driver:      d,
		key:         key,
		state:       state,
		size:        size,
		parts:       parts,
		readyPart:   ReadyPart,
		pendingPart: PendingPart,
	}
}

// 仅用于测试使用
var cnt int = 0
var num int = 0

// Implement the storagedriver.FileWriter interface
func (w *writer) Write(p []byte) (int, error) {
	// logrus.Infof(">>> Write()\n\t>>> len(p) = %v\n", len(p))
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	// If the last written part is smaller than minChunkSize, we need to make a
	// new multipart upload :sadface:
	// 如果我们保证成功上传的分块中，要么是 = 4M，要么是 < 4M（最后一块）
	// 最后一块当且仅当 Commit 时才会上传！！！
	// 因此根本不可能走到下面的逻辑里！！！
	if len(w.parts) > 0 && int((*w.parts[len(w.parts)-1]).Size) < w.state.BlkSize {
		err := w.driver.Req.FinishMultipartUpload(w.state)
		if err != nil {
			// w.driver.Req.AbortMultipartUpload(w.state)
			w.Cancel()
			return 0, err
		}

		state, err := w.driver.Req.InitiateMultipartUpload(w.key, w.driver.getContentType())
		if err != nil {
			return 0, err
		}
		w.state = state

		// If the entire written file is smaller than minChunkSize, we need to make
		// a new part from scratch :double sad face:
		if w.size < int64(w.state.BlkSize) {
			contents, err := w.driver.getContent(w.key, 0)
			if err != nil {
				w.Cancel()
				return 0, parseError(w.key, w.driver.Req.ParseError())
			}
			w.parts = nil
			w.readyPart = contents
		} else {
			// Otherwise we can use the old file as the new first part
			// TODO(zengyan) UploadPartCopy 还未实现
			// 注：若将来成功获得了 old part，一定记得在这里要修改 w.state.etag。这就意味着，sdk 需要提供一个方法来修改 state.etag 私有字段！！！
			return 0, fmt.Errorf("UploadPartCopy has yet to achieve")
		}
	}

	var n int

	for len(p) > 0 {
		// If no parts are ready to write, fill up the first part
		if neededBytes := int(w.state.BlkSize) - len(w.readyPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.readyPart = append(w.readyPart, p[:neededBytes]...)
				n += neededBytes
				num += neededBytes
				p = p[neededBytes:]
			} else { // 最后一块不足 BlkSize
				w.readyPart = append(w.readyPart, p...)
				n += len(p)
				num += len(p)
				p = nil
			}
		}

		if neededBytes := int(w.state.BlkSize) - len(w.pendingPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.pendingPart = append(w.pendingPart, p[:neededBytes]...)
				n += neededBytes
				num += neededBytes
				p = p[neededBytes:]
				logrus.Infof(">>> Write()\n\t>>> ready to flush, len(w.readyPart) = %v, len(w.pendingPart) = %v\n", len(w.readyPart), len(w.pendingPart))
				err := w.flushPart()
				if err != nil {
					w.size += int64(n)
					w.Cancel()
					return n, err
				}
			} else { // 最后一块不足 BlkSize
				w.pendingPart = append(w.pendingPart, p...)
				n += len(p)
				num += len(p)
				p = nil
			}
		}
	}

	w.size += int64(n)

	// logrus.Infof(">>> Write()\n\t>>> len(w.readyPart) = %v\n", len(w.readyPart))

	return n, nil
}

func (w *writer) Close() error {
	logrus.Infof(">>> Close()")
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	err := w.flushPart()
	if err != nil {
		return err
	}
	// 将还未上传成功的数据保存到内存中
	ReadyPart = w.readyPart
	PendingPart = w.pendingPart
	return nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Cancel() error {
	logrus.Infof("||| Cancel()\n")
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	return w.driver.Req.AbortMultipartUpload(w.state) // 删除所有分块
}

func (w *writer) Commit() error {
	logrus.Infof(">>> Commit()")
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	// 保证 Commit 时所有内存中的剩余分块都要上传
	for len(w.readyPart) > 0 {
		err := w.flushPart()
		if err != nil {
			return err
		}
	}
	// logrus.Infof(">>> Commit()\n\t>>> len(readyPart) = %v\n", len(w.readyPart))
	w.committed = true
	// logrus.Infof("||| 111\n")
	err := w.driver.Req.FinishMultipartUpload(w.state) // 分块合并为完整文件
	if err != nil {
		// logrus.Infof("||| 222, err = %v\n", err)
		err := w.driver.Req.ParseError()
		w.driver.Req.AbortMultipartUpload(w.state) // 删除所有分块
		return err
	}
	return nil
}

func (w *writer) flushPart() error {
	cnt++
	logrus.Infof(">>> flushPart()\n\t>>> cnt = %v, num = %v\n", cnt, num)
	if len(w.readyPart) == 0 && len(w.pendingPart) == 0 {
		// nothing to write
		return nil
	}

	// 若解注释掉，则最后一个 chunk 可能会大于 chunkSize。然而我们不允许 chunk 大于 chunkSize
	// if len(w.pendingPart) < int(w.driver.ChunkSize) {
	// 	// closing with a small pending part
	// 	// combine ready and pending to avoid writing a small part
	// 	w.readyPart = append(w.readyPart, w.pendingPart...)
	// 	w.pendingPart = nil
	// }

	logrus.Infof(">>> flushPart()\n\t>>> before upload, len(w.readyPart) = %v, len(w.pendingPart) = %v\n", len(w.readyPart), len(w.pendingPart))
	part, err := w.driver.Req.UploadPartRetPart(bytes.NewBuffer(w.readyPart), w.state, len(w.parts)) // 将 readyPart 上传
	if err != nil {
		return err
	}

	w.parts = append(w.parts, part)
	w.readyPart = w.pendingPart
	w.pendingPart = nil
	logrus.Infof(">>> flushPart()\n\t>>> after upload, len(w.readyPart) = %v, len(w.pendingPart) = %v\n", len(w.readyPart), len(w.pendingPart))
	return nil
}
