package file

import (
	"os"
	"testing"

	"github.com/zhangyuke-coder/zkv/utils"
)

func TestWal(t *testing.T) {
	e := &utils.Entry{
		Key:       []byte("CRTS😁硬核课堂MrGSBtL12345678"),
		Value:     []byte("我草了"),
		ExpiresAt: 123,
	}
	fileOpt := &Options{
		Dir:      "./work_test",
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    1024, //TODO wal 要设置多大比较合理？ 姑且跟sst一样大
		FID:      7,
		FileName: "./00007walFileExt",
	}
	wal := OpenWalFile(fileOpt)
	wal.Write(e)

}
