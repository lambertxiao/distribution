package us3

import (
	"context"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
)

// 以下为 distriburion 设置的测试用例
func Test(t *testing.T) { check.TestingT(t) }

var us3DriverConstructor func(rootDirectory string) (*Driver, error)

var skipCheck func() string

func init() {
	publicKey := os.Getenv("PUBLICKEY")
	privateKey := os.Getenv("PRIVATEKEY")
	api := os.Getenv("API")
	bucket := os.Getenv("BUCKET")
	regin := os.Getenv("REGIN")
	endpoint := os.Getenv("ENDPOINT")
	verifyUploadMD5Bool := os.Getenv("VERIFYUPLOADMD5")
	// ioutil.TempDir 会创建一个临时文件
	root, err := ioutil.TempDir("", "driver-")
	if err != nil {
		panic(err)
	}
	defer os.Remove(root)

	us3DriverConstructor = func(rootDirectory string) (*Driver, error) {

		verifyUploadMD5 := false

		if verifyUploadMD5Bool != "" {
			verifyUploadMD5, err = strconv.ParseBool(verifyUploadMD5Bool)
			if err != nil {
				return nil, err
			}
		}

		param := DriverParameters{
			PublicKey:       publicKey,
			PrivateKey:      privateKey,
			Api:             api,
			Bucket:          bucket,
			Regin:           regin,
			Endpoint:        endpoint,
			VerifyUploadMD5: verifyUploadMD5,
			RootDirectory:   rootDirectory,
		}
		return New(param)
	}

	skipCheck = func() string {
		if publicKey == "" || privateKey == "" || bucket == "" || regin == "" || endpoint == "" {
			return "Must set PUBLICKEY, PRIVATEKEY, API, BUCKET, REGIN and ENDPOINT to run us3 tests"
		}
		return ""
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return us3DriverConstructor(root)
	}, skipCheck)
}

// // 以下为自己写的测试用例
// func TestPutContent(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	contents := []byte("contents")
// 	path := "/dir/file"
// 	err = driver.PutContent(context.Background(), path, contents)
// 	if err != nil {
// 		t.Fatalf("unexpected error put content: %v", err)
// 	}

// 	path = "/dir/filesuffix"
// 	err = driver.PutContent(context.Background(), path, contents)
// 	if err != nil {
// 		t.Fatalf("unexpected error put content: %v", err)
// 	}

// 	path = "/dir/dir/file"
// 	err = driver.PutContent(context.Background(), path, contents)
// 	if err != nil {
// 		t.Fatalf("unexpected error put content: %v", err)
// 	}

// 	path = "/dir/dirsuffix/file"
// 	err = driver.PutContent(context.Background(), path, contents)
// 	if err != nil {
// 		t.Fatalf("unexpected error put content: %v", err)
// 	}
// }

func TestDelete(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	rootDirectory := os.Getenv("ROOTDIRECTORY")
	// driver, err := us3DriverConstructor(rootDirectory)
	driver, err := us3DriverConstructor("/")
	if err != nil {
		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
	}

	path := "/var"
	err = driver.Delete(context.Background(), path)
	if err != nil {
		t.Fatalf("unexpected error delete content: %v", err)
	}
}

// func TestGetContent(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	path := "/dir"
// 	_, err = driver.GetContent(context.Background(), path)
// 	// content, err := driver.GetContent(context.Background(), path)
// 	if err != nil {
// 		t.Fatalf("unexpected error get content: %v", err)
// 	}
// 	// t.Logf("get content success, content=%v", content)
// 	// logrus.Infof(">>> content=%v", string(content))
// 	logrus.Infof(">>> get content succeed\n")
// }

// func TestURLFor(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	path := "/test"
// 	URL1, err := driver.URLFor(context.Background(), path, map[string]interface{}{"method": "GET"})
// 	if err != nil {
// 		t.Fatalf("unexpected error get URL1: %v", err)
// 	}
// 	t.Logf("get URL success, URL1=%v", URL1)
// 	URL2, err := driver.URLFor(context.Background(), path, map[string]interface{}{"method": "GET", "expiry": 10 * time.Minute})

// 	if err != nil {
// 		t.Fatalf("unexpected error get URL2: %v", err)
// 	}
// 	t.Logf("get URL success, URL2=%v", URL2)
// }

// func TestMove(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	sourcePath := "/test1"
// 	destPath := "/dir/test1"
// 	err = driver.Move(context.Background(), sourcePath, destPath)
// 	if err != nil {
// 		t.Fatalf("unexpected error move file: %v", err)
// 	}
// 	t.Logf("move file success, source path is %v, dest path is %v", sourcePath, destPath)
// }

// func TestStat(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	path := "/di"
// 	fileInfo, err := driver.Stat(context.Background(), path)
// 	if err != nil {
// 		t.Fatalf("unexpected error stat file: %v", err)
// 	} else if fileInfo.IsDir() {
// 		t.Fatalf("unexpected error stat file: Stat() return path is a dir. It means %v is not exist", path)
// 	}
// 	t.Logf("stat file success, file is %v, size is %v, modtime is %v", path, fileInfo.Size(), fileInfo.ModTime())
// }

// func TestList(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	// path := "/dir/dir1/" // error
// 	// path := "/var/folders/2z/vtlyvcts60d82ynnxywknyjc0000gn/T/driver-1489514393"
// 	path := "/dir/file"
// 	files, err := driver.List(context.Background(), path)
// 	if err != nil {
// 		t.Fatalf("unexpected error list file: %v", err)
// 	}
// 	t.Logf("list file success, files are %v", files)
// }

// var ct int

// func f(fileInfo storagedriver.FileInfo) error {
// 	ct++
// 	fmt.Printf("> f()\n")
// 	fmt.Printf(">> ct = %v\n", ct)
// 	if fileInfo.IsDir() {
// 		// return fmt.Errorf(">> %v is dir!\n", fileInfo.Path())
// 		fmt.Printf(">> %v is dir!\n", fileInfo.Path())
// 		return nil
// 	}
// 	fmt.Printf(">> %v is not dir\n", fileInfo.Path())
// 	fmt.Printf(">> size = %v\n", fileInfo.Size())
// 	fmt.Printf(">> modtime = %v\n", fileInfo.ModTime())
// 	return nil
// }

// func TestWalk(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	path := "/dir"
// 	err = driver.Walk(context.Background(), path, f)
// 	if err != nil {
// 		t.Fatalf("unexpected error walk: %v", err)
// 	}
// 	t.Logf("walk success")
// }

// func TestReader(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	path := "/11.txt"
// 	reader, err := driver.Reader(context.Background(), path, 5)
// 	if err != nil {
// 		t.Fatalf("unexpected error reader: %v", err)
// 	}
// 	content, _ := ioutil.ReadAll(reader)
// 	t.Logf("reader success, content=%v", content)
// 	logrus.Infof(">>> content=%v", string(content))
// 	reader.Close()
// }

// func generateBigfile(filepath string, fsize int) {
// 	// 注：fsize 单位为 MB
// 	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0755)
// 	if err != nil {
// 		panic("unexpected error creating Bigfile: " + err.Error())
// 	}
// 	size := (1 << 20) * fsize
// 	err = f.Truncate(int64(size))
// }

// func TestWriter(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	filepath := "./Bigfile"
// 	generateBigfile(filepath, 10)
// 	file, err := os.OpenFile(filepath, os.O_RDWR, 0755)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}

// 	path := "/Bigfile"
// 	writer, err := driver.Writer(context.Background(), path, false)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	_, err = io.Copy(writer, file)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	err = writer.Commit()
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	err = writer.Close()
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	t.Logf("writer success")
// }

// func TestAppendWriter(t *testing.T) {
// 	if skipCheck() != "" {
// 		t.Skip(skipCheck())
// 	}

// 	rootDirectory := os.Getenv("ROOTDIRECTORY")
// 	driver, err := us3DriverConstructor(rootDirectory)
// 	if err != nil {
// 		t.Fatalf("unexpected error creating driver with ROOT=%s: %v", rootDirectory, err)
// 	}

// 	filepath := "./Bigfile"
// 	generateBigfile(filepath, 10)
// 	file, err := os.OpenFile(filepath, os.O_RDWR, 0755)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}

// 	path := "/Bigfile6"

// 	writer, err := driver.Writer(context.Background(), path, false)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	_, err = io.Copy(writer, file)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	err = writer.Close()
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}

// 	logrus.Infof(">>> finish 1")

// 	file.Seek(0, 0)
// 	writer, err = driver.Writer(context.Background(), path, true)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	_, err = io.Copy(writer, file)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	err = writer.Close()
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}

// 	logrus.Infof(">>> finish 2")

// 	file.Seek(0, 0)
// 	writer, err = driver.Writer(context.Background(), path, true)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	_, err = io.Copy(writer, file)
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}

// 	logrus.Infof(">>> finish 3")

// 	err = writer.Commit()
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	err = writer.Close()
// 	if err != nil {
// 		t.Fatalf("unexpected error writer: %v", err)
// 	}
// 	t.Logf("writer success")
// 	logrus.Infof(">>> finish total")
// }
