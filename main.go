/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"log"

	"github.com/arosyjc/goclaw/cfg"
	"github.com/arosyjc/goclaw/cmd"
	"github.com/spf13/viper"
)

func main() {
	// 读取全局配置
	v := viper.New()
	v.SetConfigFile("./goclaw.toml")
	v.SetConfigType("toml")
	if err := v.ReadInConfig(); err != nil {
		log.Fatalf("读取toml失败: %v", err)
	}
	var conf cfg.Config
	if err := v.Unmarshal(&conf); err != nil {
		panic(err)
	}
	cmd.Execute()
}
