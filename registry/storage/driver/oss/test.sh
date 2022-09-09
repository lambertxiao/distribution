#!/bin/bash

export ALIYUN_ACCESS_KEY_ID=LTAI5tFVUcHQupLBe9xSGeCP
export ALIYUN_ACCESS_KEY_SECRET=M3pB4knqbmeYt6A5j2e6Vi4VpeBnBR
export OSS_BUCKET=bulabula-test-bigfile
export OSS_REGION=oss-cn-shanghai
export OSS_ENDPOINT=bulabula-test-bigfile.oss-cn-shanghai.aliyuncs.com
export OSS_ENCRYPT=false
export ROOTDIRECTORY=/my_images

# test_project="TestEmptyRootList"
# test_project="TestPutContent"
# test_project="TestDelete"
# test_project="TestGetContent"
# test_project="TestURLFor"
# test_project="TestMove"
# test_project="TestStat" 
# test_project="TestList"
# test_project="TestWalk"
# test_project="TestReader"
# test_project="TestWriter"
# test_project="TestAppendWriter"

project=""


# go test -timeout 30s -v -run ${test_project}
# go test -v -timeout 30s -check.f ${project}
# go test -v -check.f ${project} # 对指定 project 进行测试，还会测试 xxx_test.go 文件中所有 Testxxx
go test -v -check.b ${project} # 对指定 project 进行测试，还会测试 xxx_test.go 文件中所有 Testxxx

# go test -v . # 对所有测试用例进行测试，包括 distribution 中的