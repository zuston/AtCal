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
	commandLine := "hadoop jar /home/hadoop/jobs/AtCal/target/atcal-jar-with-dependencies.jar ane " + *time + " " + strconv.Itoa(*section)
	execCommand(commandLine)
}

func execCommand(commandLine string) bool {
	cmd := exec.Command("/bin/bash","-c",commandLine)

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