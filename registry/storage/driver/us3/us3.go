package us3

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	ufsdk "github.com/ufilesdk-dev/ufile-gosdk"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
)

// TODO(zengyan) NOW
// - RUN 起来
// - 列出缺少的 sdk
// - reader
//   - 几个东西的转换
// - Move & Delete
//   - 具体要删除/移动哪些文件
// - Stat & List
// 	 - 具体要显示哪些文件
//   - path 到底是什么
//	 - 有一些特殊情况需要考虑
// - writer
// - config
// 	 - 哪些是可选的
// - us3_test.go

const driverName = "us3"

type DriverParameters struct {
	PublicKey       string // *************
	PrivateKey      string // *************
	Api             string // api.ucloud.cn
	Bucket          string // test-hzy
	Regin           string // cn-sh2
	Endpoint        string // cn-sh2.ufileos.com
	VerifyUploadMD5 bool   // false
	Rootdirectory   string // /my_images 或 /
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
	// TODO(zengyan) driver struct
	Req             *ufsdk.UFileRequest
	PublicKey       string
	PrivateKey      string
	Api             string
	Bucket          string
	Endpoint        string
	VerifyUploadMD5 bool
	Rootdirectory   string
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	// TODO(zengyan) 哪些是 option 的，需要对是否为 nil "" bool 进行判断
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
		return nil, fmt.Errorf("No Api parameter provided")
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
	if !ok {
		verifyUploadMD5, ok = verifyUploadMD5Bool.(bool)
		return nil, fmt.Errorf("No VerifyUploadMD5 parameter provided")
	}
	rootdirectory, ok := parameters["Rootdirectory"]
	if !ok {
		return nil, fmt.Errorf("No Rootdirectory parameter provided")
	}

	param := DriverParameters{
		PublicKey:       fmt.Sprint(publicKey),
		PrivateKey:      fmt.Sprint(privateKey),
		Api:             fmt.Sprint(api),
		Bucket:          fmt.Sprint(bucket),
		Regin:           fmt.Sprint(regin),
		Endpoint:        fmt.Sprint(endpoint),
		VerifyUploadMD5: verifyUploadMD5,
		Rootdirectory:   fmt.Sprint(rootdirectory),
	}
	return New(param)
}

func New(params DriverParameters) (*Driver, error) {
	config := &ufsdk.Config{
		PublicKey:       params.PublicKey,
		PrivateKey:      params.PrivateKey,
		BucketHost:      params.Api,
		BucketName:      params.Bucket,
		FileHost:        params.Endpoint,
		VerifyUploadMD5: params.VerifyUploadMD5,
		Endpoint:        params.Endpoint,
	}

	req, err := ufsdk.NewFileRequest(config, nil)
	if err != nil {
		return nil, err
	}

	d := &driver{
		Req:           req,
		Rootdirectory: params.Rootdirectory,
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
	logrus.Infof(">> GetContent()")
	// 默认采取流式下载
	// TODO(zengyan) 以下的方法是否可优化？拷贝次数太多？
	buf := bytes.NewBuffer(nil)
	writer := bufio.NewWriter(buf)
	err := d.Req.DownloadFile(writer, path)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	return buf.Bytes(), err
	// TODO(zengyan) get 为什么没有 NoSuchKey 的逻辑？？？
}

// 上传 path 所对应的 image
// 注：具体的 bucket 信息存放在 d 中
// 注：path 为 image 的名字
// 如：/hello-world，那么该文件的 key 就应该为 /my_images/hello-world
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	logrus.Infof(">> PutContent()")
	if len(contents) >= 4*1024*1024 { // contents >= 4M 采用分片流式上传
		return d.Req.IOMutipartAsyncUpload(bytes.NewReader(contents), d.us3Path(path), d.getContentType())
	} else { // contents < 4M 采用普通流式上传
		return d.Req.IOPut(bytes.NewReader(contents), d.us3Path(path), d.getContentType())
	}
	// TODO(zengyan) put 为什么没有 NoSuchKey 的逻辑？？？
}

func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	fmt.Printf(">> Reader() is not implemented yet\n")
	return nil, nil

	// header := make(http.Header)
	// // TODO(zengyan) 不确定 header 格式是否正确
	// header.Add("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")

	// var err error
	// d.Req, err = ufsdk.NewFileRequestWithHeader(&ufsdk.Config{
	// 	PublicKey:       d.PublicKey,
	// 	PrivateKey:      d.PrivateKey,
	// 	BucketHost:      d.Api,
	// 	BucketName:      d.Bucket,
	// 	FileHost:        d.Endpoint,
	// 	VerifyUploadMD5: d.VerifyUploadMD5,
	// 	Endpoint:        d.Endpoint,
	// }, header, nil)

	// if err != nil {
	// 	return nil, err
	// }

	// // TODO(zengyan) reader writer readcloser []byte 之间的转换
	// buf := bytes.NewBuffer(nil)
	// writer := bufio.NewWriter(buf)
	// err = d.Req.DownloadFile(writer, d.us3Path(path))
	// if err != nil {
	// 	return nil, err
	// }

	// var reader io.ReadCloser
	// io.Copy(writer, reader)
	// return reader, nil

	// TODO(zengyan) put 为什么没有 NoSuchKey 的逻辑？？？
}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	fmt.Printf(">> Writer() is not implemented yet\n")
	return nil, nil
}

func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	// TODO(zengyan) 确定一下 delim 是否为 ""
	list, err := d.Req.ListObjects(d.us3Path(path), "", "", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(list.Contents) == 1 {
		// TODO(zengyan) 由于 ListObjects 是前缀匹配，存在一种情况，path 为真实存在的文件名，但是用户输入 path 时末尾漏了一位。这将导致程序返回 path is a dir
		if list.Contents[0].Key != d.us3Path(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			size, err := strconv.ParseInt(list.Contents[0].Size, 10, 64)
			if err != nil {
				return nil, err
			}
			fi.Size = size
			// TODO(zengyan) 不确定这里返回的 int 类型的 LastModified 是否能按 RFC3339Nano 求出 time.time 类型的值
			timestamp, err := time.Parse(time.RFC3339Nano, strconv.Itoa(list.Contents[0].LastModified))
			if err != nil {
				return nil, err
			}
			fi.ModTime = timestamp
		}
	} else if len(list.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	fmt.Printf(">> List() is not implemented yet\n")
	return nil, nil
}

// 将 sourcePath 对应的文件移动至 destPath 对应的文件
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	err := d.Req.Copy(d.us3Path(destPath), d.Bucket, d.us3Path(sourcePath))
	if err != nil {
		return err
	}
	return d.Delete(ctx, sourcePath)
}

// 删除 path 对应的文件
func (d *driver) Delete(ctx context.Context, path string) error {
	// TODO(zengyan) 需要确定到底要删什么东西？
	return d.Req.DeleteFile(d.us3Path(path))
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
// * 猜测：path 是一个目录路径，而不是一个文件路径
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

// 将 d.RootDirectory 与 path 拼起来
// 用于获取 path 对应文件的完整 key
// * 猜测：path 为 image 的文件名
// 若要 put 的 image 为 106.75.215.32:8080/library/hello-world，则 path 为 /library/hello-world
// 调用 us3Path 后返回 d.RootDirectory+path。即：/my_images/library/hello-world 或 /library/hello-world
func (d *driver) us3Path(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.Rootdirectory, "/")+path, "/")
}

// ============================= writer ==================================

type writer struct {
	// TODO(zengyan) writer struct
}

// Implement the storagedriver.FileWriter interface
func (w *writer) Write(p []byte) (int, error) {
	fmt.Printf(">> Write() is not implemented yet\n")
	return 0, nil
}

func (w *writer) Close() error {
	fmt.Printf(">> Close() is not implemented yet\n")
	return nil
}

func (w *writer) Size() int64 {
	fmt.Printf(">> Size() is not implemented yet\n")
	return 0
}

func (w *writer) Cancel() error {
	fmt.Printf(">> Cancel() is not implemented yet\n")
	return nil
}

func (w *writer) Commit() error {
	fmt.Printf(">> Commit() is not implemented yet\n")
	return nil
}
