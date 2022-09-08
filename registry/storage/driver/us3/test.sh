#!/bin/bash

export PUBLICKEY=TOKEN_cbdec267-2c4a-4604-804e-caf5873bb519
export PRIVATEKEY=bdbea8cc-655b-4d56-8f6d-1b8b7c595fe6
# export API=api.ucloud.cn # 可选
export BUCKET=lambert-wlcb
export REGIN=cn-wlcb
export ENDPOINT=cn-wlcb.ufileos.com
# export VERIFYUPLOADMD5=false # 可选
export ROOTDIRECTORY=/my_images

# test_project="TestEmptyRootList"
# test_project="TestPutContent"
test_project="TestDelete"
# test_project="TestGetContent"
# test_project="TestURLFor"
# test_project="TestMove"
# test_project="TestStat"
# test_project="TestList"
# test_project="TestWalk"
# test_project="TestReader"
# test_project="TestWriter"
# test_project="TestAppendWriter"

# project="BenchmarkDelete50Files"


go test -timeout 30s -v -run ${test_project}
# go test -v -timeout 30s -check.f ${project}
# go test -v -check.f ${project} # 对指定 project 进行测试，还会测试 xxx_test.go 文件中所有 Testxxx
# go test -v -check.b ${project} # 对指定 project 进行测试，还会测试 xxx_test.go 文件中所有 Testxxx

# go test -v . # 对所有测试用例进行测试，包括 distribution 中的