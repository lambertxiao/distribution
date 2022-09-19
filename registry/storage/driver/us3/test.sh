#!/bin/bash

export PUBLICKEY=TOKEN_cbdec267-2c4a-4604-804e-caf5873bb519
export PRIVATEKEY=bdbea8cc-655b-4d56-8f6d-1b8b7c595fe6
# export API=api.ucloud.cn # 可选
export BUCKET=lambert-wlcb
export REGIN=cn-wlcb
export ENDPOINT=cn-wlcb.ufileos.com
# export VERIFYUPLOADMD5=false # 可选
export ROOTDIRECTORY=/my_images # 可选

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
test_project="TestAppendWriter"

projects=(
"riverSuite.TestContinueStreamAppendLarge"
"riverSuite.TestContinueStreamAppendSmall"
"riverSuite.TestDelete"
"riverSuite.TestDeleteFolder"
"riverSuite.TestDeleteNonexistent"
"riverSuite.TestDeleteOnlyDeletesSubpaths"
"riverSuite.TestInvalidPaths"
"riverSuite.TestList"
"riverSuite.TestMove"
"riverSuite.TestMoveInvalid"
"riverSuite.TestMoveNonexistent"
"riverSuite.TestMoveOverwrite"
"riverSuite.TestPutContentMultipleTimes"
"riverSuite.TestReadNonexistent"
"riverSuite.TestReadNonexistentStream"
"riverSuite.TestReaderWithOffset"
"riverSuite.TestRootExists"
"riverSuite.TestStatCall"
"riverSuite.TestTruncate"
"riverSuite.TestURLFor"
"riverSuite.TestValidPaths"
"riverSuite.TestWriteRead1"
"riverSuite.TestWriteRead2"
"riverSuite.TestWriteRead3"
"riverSuite.TestWriteRead4"
# "riverSuite.TestWriteReadLargeStreams" # 5G -> 500MB
"riverSuite.TestWriteReadNonUTF8"
"riverSuite.TestWriteReadStreams1"
"riverSuite.TestWriteReadStreams2"
"riverSuite.TestWriteReadStreams3"
"riverSuite.TestWriteReadStreams4"
"riverSuite.TestWriteReadStreamsNonUTF8"
# "riverSuite.TestConcurrentFileStreams" # 32MB * 32 -> 5MB * 5
# "riverSuite.TestConcurrentStreamReads" # 128MB * 10 -> 10MB * 10
)

echo "++++++++ ${#projects[*]} projects ++++++++"

for pj in ${projects[*]}
do
    go test -v -check.f ${pj} 
done

project="TestConcurrentFileStreams"

# go test -v -run ${test_project}
# go test -check.f ${project}
# go test -v -check.list # 列出所有Test
# go test -v -check.f ${project} # 对指定 project 进行测试，还会测试 xxx_test.go 文件中所有 Testxxx
# go test -v -check.b # 测试所有 Benchmarkxxx，不还会测试 xxx_test.go 文件中的 Testxxx

# go test -v . # 对所有测试用例进行测试，包括 distribution 中的