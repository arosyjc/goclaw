package cfg

// 配置
type Config struct {
	Server Server `mapstructure:"server"`
}

// 服务配置
type Server struct {
	HttpAddr        string `mapstructure:"httpAddr"`
	MaxConnNum      uint16 `mapstructure:"maxConnNum"`
	WriteChanBuf    uint16 `mapstructure:"writeChanBuf"`
	MaxReadMsgSize  uint16 `mapstructure:"maxReadMsgSize"`
	HeartbeatPeriod uint8  `mapstructure:"heartbeatPeriod"`
	ReadDeadline    uint8  `mapstructure:"readDeadline"`
	WriteDeadline   uint8  `mapstructure:"writeDeadline"`
	//消息类型
	MsgTypeText   uint8 `mapstructure:"msgTypeText"`
	MsgTypeBinary uint8 `mapstructure:"msgTypeBinary"`
	MsgTypePing   uint8 `mapstructure:"msgTypePing"`
	MsgTypePong   uint8 `mapstructure:"msgTypePong"`
}
