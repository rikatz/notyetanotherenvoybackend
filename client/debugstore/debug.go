package debugstore

import (
	"context"
	"log/slog"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	jsonMarshaler = protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: true, // Print empty fields too
	}
)

type NotifyMsg struct {
	Operation        string
	Resources        []proto.Message
	ResourcesRemoved []string
}

func debug(ctx context.Context, msg proto.Message) {
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		resValue, err := jsonMarshaler.Marshal(msg)
		if err != nil {
			slog.Warn("error trying to marshal for debug", "error", err)
			return
		}
		slog.Debug("xDS resource details", "json", string(resValue))
	}
}
