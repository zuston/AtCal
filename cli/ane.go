package main

import (
	"flag"
	"fmt"
	"bufio"
	"io"
	"os/exec"
	"strconv"
)

var time = flag.String("time", "2017-10-10", "the current time that you set")
var section = flag.Int("section",7,"the section that you set to get the data, which is serve for the develop mode")

func main(){
	flag.Parse()
	//commandName := "hadoop"
	//params := []string{"jar",
	//"/home/hadoop/jobs/AtCal/target/atcal-jar-with-dependencies.jar",
	//	"ane",
	//	*time,
	//	strconv.Itoa(*section),
	//}
	commandLine := "hadoop jar /home/hadoop/jobs/AtCal/target/atcal-jar-with-dependencies.jar ane "+*time + " " + strconv.Itoa(*section)
	commandLine = "git clone https://github.com/puniverse/quasar.git"
	res := execCommand(commandLine)
	fmt.Println(res)
}

func execCommand(commandName string) bool {
	//函数返回一个*Cmd，用于使用给出的参数执行name指定的程序
	cmd := exec.Command("/bin/bash","-c",commandName)
	//显示运行的命令
	fmt.Println(cmd.Args)

	stdout, err := cmd.StdoutPipe()

	if err != nil {
		fmt.Println(err)
		return false
	}

	cmd.Start()

	reader := bufio.NewReader(stdout)

	for {
		line, err2 := reader.ReadString('\n')
		if err2 != nil || io.EOF == err2 {
			break
		}
		fmt.Println(line)
	}
	cmd.Wait()
	return true
}

