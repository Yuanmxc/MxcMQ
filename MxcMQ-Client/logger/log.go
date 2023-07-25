package logger

import (
	"io"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var log *zap.SugaredLogger

var logLevel = zap.NewAtomicLevel()

func init() {
	w1 := getLogWriter()
	e1 := getEncoder()

	core := zapcore.NewCore(
		e1,
		w1,
		logLevel,
	)

	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	log = logger.Sugar()
}

func getLogWriter() zapcore.WriteSyncer {
	file, _ := os.OpenFile("./test.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	ws := io.MultiWriter(file, os.Stdout)
	return zapcore.AddSync(ws)
}

func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return zapcore.NewJSONEncoder(encoderConfig)
}
